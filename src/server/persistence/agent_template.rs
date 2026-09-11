//! Durable revisions for agent TEMPLATES -- RF-ADR-008 item C.
//!
//! A template is a published agent plus declared axes of variation: the same
//! role on a cheaper model, the same research agent against a different search
//! tool. See [`eg_types::agent_template`] for why it is a complete base plus
//! substitutions rather than a partially-specified agent.
//!
//! Published into the SAME owner file as [`super::agent_component`],
//! [`super::agent_library`] and [`super::agent_graph`]. A template is a
//! GENERATOR of ordinary library entries, not a separate entity family, so a
//! fourth physical store would split one authority in two (RF-RULING-004) --
//! and "which agents came from this template?" would have to be answered by
//! reconciling two files.
//!
//! The one operation this layer adds over the three beside it is
//! [`AgentLibraryStore::instantiate_template`]: it binds parameters and returns
//! an ordinary `AgentLibraryEntryDraft`. It is a READ -- nothing is committed
//! until the caller publishes that draft through the agent library -- and that
//! is exactly what keeps admission and delegation free of a template-aware
//! branch.
//!
//! The revision protocol is the same one the other three layers use and reuses
//! the same machinery from [`super::agent_library`].

use std::collections::BTreeMap;
use std::sync::Arc;

use redb::ReadableTable;

use eg_storage::{OwnedStoreHandle, RecordedOperation, ScopedRead};
use eg_transaction::{AdmittedOwnerWrite, Begin, ReplayResolution};

use eg_types::agent_template::{
    AgentTemplateCommittedResult, AgentTemplateEntry, AgentTemplateMutationKind,
    AgentTemplateOutboxEvent, AgentTemplatePublishRequest, AgentTemplateRetireRequest,
    AgentTemplateStatusRequest, AGENT_TEMPLATE_SCHEMA_VERSION,
};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::mutation::MutationResult;
use eg_types::mutation_batch::{
    BatchContent, CompiledEnvelope, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationBatch, MutationEnvelope, MutationOperation, MutationOutboxIntent, MutationSurface,
    VersionExpectation, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;

use super::agent_library::{
    admitted_context, agent_library_operation_identity, batch_id,
    effective_agent_library_policy_digest, next_revision, owner_receipt, require_expected_revision,
    resolve_nonce_first, validate_context, AgentLibraryStore,
};

const AGENT_TEMPLATE_OUTBOX_TOPIC: &str = "eg.agent-template.revision.v1";
const AGENT_TEMPLATE_RESULT_SCHEMA_ID: &str = "agent-template-result.v1";
const MAX_AGENT_TEMPLATE_ROW_BYTES: usize = 16 * 1024 * 1024;
const MAX_AGENT_TEMPLATE_ROW_ITEMS: usize = 200_000;
const MAX_AGENT_TEMPLATE_REVISIONS: usize = 16_384;
const MAX_AGENT_TEMPLATE_HISTORY_BYTES: usize = 256 * 1024 * 1024;

/// What a committed template write returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTemplateWriteResult {
    pub result: AgentTemplateCommittedResult,
    pub replayed: bool,
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
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent template writes require an explicit expected_revision".to_string())?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;

        // Every early return past this point must abort the transaction: an
        // open write that is neither committed nor aborted holds the owner's
        // write lock for the life of the process.
        let nonce = match resolve_nonce_first(&self.mutations, &txn, &request.context) {
            Ok(nonce) => nonce,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let next = match next_revision(expected_revision) {
            Ok(next) => next,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        // The replay identity is minted from the DEFINITION the caller asked
        // for, so a byte-identical retry resolves to the same operation rather
        // than becoming a second revision. Computed before the entry exists,
        // exactly as `agent_graph` mints its identity from its draft digest.
        let definition_digest = eg_types::agent_template::draft_definition_digest(&request.template);
        let replay_context =
            match admitted_context(&request.context, "agent-template:publish") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "template-publish",
            &request.template.template_id,
            expected_revision,
            Some(&definition_digest),
        ) {
            Ok(operation) => operation,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay = match self.mutations.resolve_replay(&txn, &operation, &nonce) {
            Ok(replay) => replay,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some(result) = match replayed_template(replay, &request.template.template_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        let entry = match AgentTemplateEntry::create(
            request.template,
            next,
            AgentLibraryLifecycle::Published,
            replay_context.created_at_ms,
            replay_context.created_at_ms,
        ) {
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
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent template writes require an explicit expected_revision".to_string())?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        let nonce = match resolve_nonce_first(&self.mutations, &txn, &request.context) {
            Ok(nonce) => nonce,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let next = match next_revision(expected_revision) {
            Ok(next) => next,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay_context =
            match admitted_context(&request.context, "agent-template:retire") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "template-retire",
            &request.template_id,
            expected_revision,
            None,
        ) {
            Ok(operation) => operation,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let replay = match self.mutations.resolve_replay(&txn, &operation, &nonce) {
            Ok(replay) => replay,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        if let Some(result) = match replayed_template(replay, &request.template_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        let current = match self.template_at_revision_in_write(
            &txn,
            &request.context.tenant_id,
            &request.template_id,
            expected_revision,
        ) {
            Ok(Some(current)) => current,
            Ok(None) => {
                txn.abort()?;
                return Err("agent template has no revision to retire".to_string());
            }
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let tombstone = match current.retire(next, replay_context.created_at_ms) {
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

    /// The head revision of one template, or `None` if it was never published.
    pub fn current_template(
        &self,
        tenant_id: &str,
        template_id: &str,
    ) -> Result<Option<AgentTemplateEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, template_id)?;
        let read = self.read()?;
        Ok(read_template_history(&read, tenant_id, template_id)?
            .1
            .into_iter()
            .last())
    }

    /// Every retained revision of one template, oldest first.
    pub fn template_revisions(
        &self,
        tenant_id: &str,
        template_id: &str,
    ) -> Result<Vec<AgentTemplateEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, template_id)?;
        let read = self.read()?;
        Ok(read_template_history(&read, tenant_id, template_id)?.1)
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn template_status(
        &self,
        request: AgentTemplateStatusRequest,
    ) -> Result<Option<AgentTemplateWriteResult>, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.template_id)?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let batch_id = batch_id(&request.context.idempotency_key)?;
        let read = self.kernel.read_scope(&owner)?;
        let Some(record) = eg_transaction::read_ledger(&read, &batch_id)? else {
            return Ok(None);
        };
        let Some(result_bytes) = record.result_msgpack.as_ref() else {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent template status has no typed result".to_string(),
            );
        };
        let committed = decode_committed_result(result_bytes)?;
        if committed.template.template_id != request.template_id {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent template status resolved a different template"
                    .to_string(),
            );
        }
        Ok(Some(AgentTemplateWriteResult {
            result: committed,
            replayed: true,
        }))
    }

    /// Bind a template's parameters and return an ordinary agent draft.
    ///
    /// The query this layer exists for, and a READ: it resolves durable state
    /// and computes, but commits nothing. The caller publishes the returned
    /// draft through the agent library exactly as it would a hand-authored
    /// one, which is what keeps admission and delegation free of any
    /// template-aware branch.
    ///
    /// `entry_revision` pins which revision to bind; `None` means the head.
    ///
    /// The LIFECYCLE consulted is always the HEAD's, never the pinned
    /// revision's. A tombstone is a separate LATER revision, so a pinned
    /// revision stays `Published` in its own row forever -- checking it would
    /// let a withdrawn template keep minting agents indefinitely, which is
    /// precisely what retiring it was meant to stop. `AgentTemplateEntry::
    /// instantiate` also refuses a retired entry, but that check can only see
    /// the row it was called on; this one is the gate that matters.
    pub fn instantiate_template(
        &self,
        request: &eg_types::agent_template::AgentTemplateInstantiateRequest,
    ) -> Result<eg_types::agent_library::AgentLibraryEntryDraft, String> {
        request.validate()?;
        eg_types::agent_library::validate_key(&request.tenant_id, &request.template_id)?;
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::AGENT_TEMPLATE_HEADS)?;
        let head_revision = heads
            .get((request.tenant_id.as_str(), request.template_id.as_str()))
            .map_err(|error| error.to_string())?
            .map(|value| value.value())
            .ok_or_else(|| "no such agent template in this tenant".to_string())?;
        let revisions = read.open_owner_table(eg_storage::AGENT_TEMPLATE_REVISIONS)?;
        let head = revisions
            .get((
                request.tenant_id.as_str(),
                request.template_id.as_str(),
                head_revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent template head points to a missing revision".to_string())?;
        let head_lifecycle = decode_template(head.value())?.lifecycle;
        if head_lifecycle == AgentLibraryLifecycle::Retired {
            return Err("a retired agent template cannot be instantiated".to_string());
        }
        let revision = request.entry_revision.unwrap_or(head_revision);
        let pinned = revisions
            .get((
                request.tenant_id.as_str(),
                request.template_id.as_str(),
                revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "no such retained revision of that agent template".to_string())?;
        let entry = decode_template(pinned.value())?;
        if entry.tenant_id != request.tenant_id
            || entry.template_id != request.template_id
            || entry.entry_revision != revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent template row does not match its physical key"
                    .to_string(),
            );
        }
        entry.instantiate(&request.agent_id, &request.bindings)
    }

    fn template_at_revision_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        template_id: &str,
        revision: u64,
    ) -> Result<Option<AgentTemplateEntry>, String> {
        if revision == 0 {
            return Ok(None);
        }
        let revisions = write.open_read_table(eg_storage::AGENT_TEMPLATE_REVISIONS)?;
        let Some(value) = revisions.get((tenant_id, template_id, revision))? else {
            return Ok(None);
        };
        let entry = decode_template(value.value())?;
        if entry.tenant_id != tenant_id
            || entry.template_id != template_id
            || entry.entry_revision != revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent template row does not match its physical key"
                    .to_string(),
            );
        }
        Ok(Some(entry))
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
        let operations = template_operations(kind, &entry);
        let policy_digest = effective_agent_library_policy_digest(&operations)?;
        let mut admitted = context.clone();
        admitted.policy_digest = format!("sha256:{}", policy_digest.to_hex());

        let event = AgentTemplateOutboxEvent {
            schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
            kind,
            template: entry.clone(),
            performing_actor: admitted.caller_principal.clone(),
            action_actor_scope: admitted.actor_scope.clone(),
        };
        event.validate()?;
        let event_bytes = eg_storage::encode_bounded(&event, "agent template outbox event")?;
        let entry_bytes = eg_storage::encode_bounded(&entry, "agent template revision")?;
        let batch_id = batch_id(&admitted.idempotency_key)?;

        let authoritative_version = match self.mutations.current_version(&txn, owner) {
            Ok(version) => version,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let batch = match build_template_batch(
            owner,
            &admitted,
            kind,
            &entry,
            authoritative_version,
            &batch_id,
            event_bytes.clone(),
        ) {
            Ok(batch) => batch,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let begun = match txn.begin_with_replay_identity(&batch, operation, nonce) {
            Ok(begun) => begun,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let source_version = match begun {
            Begin::Apply {
                source_version: Some(source_version),
            } => source_version,
            Begin::Apply {
                source_version: None,
            } => {
                txn.abort()?;
                return Err("agent template admission has no native source version".to_string());
            }
            Begin::Replay(_) => {
                txn.abort()?;
                return Err("CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision".to_string());
            }
        };
        if source_version != authoritative_version {
            txn.abort()?;
            return Err("agent template source version changed while admitting write".to_string());
        }
        let committed_version = match source_version.checked_add(1) {
            Some(version) => version,
            None => {
                txn.abort()?;
                return Err("agent template committed version overflow".to_string());
            }
        };
        let stable_result = AgentTemplateCommittedResult {
            schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
            template: entry.clone(),
            batch_id: batch_id.clone(),
            committed_version,
        };
        let result_bytes = match encode_template_domain_result(&stable_result) {
            Ok(bytes) => bytes,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };

        let owner_write_result: Result<(), String> = (|| {
            let owner_write = txn.owner_rows(owner, &batch)?;
            apply_template_rows(
                &owner_write,
                &admitted,
                expected_revision,
                &entry,
                entry_bytes.as_slice(),
            )?;
            owner_write.finish_owner()
        })();
        if let Err(error) = owner_write_result {
            txn.abort()?;
            return Err(error);
        }

        let key = format!(
            "{}:{}:{}",
            entry.tenant_id, entry.template_id, entry.entry_revision
        );
        let headers = template_outbox_headers(&entry);
        let receipt = match owner_receipt(
            operation,
            nonce,
            &batch,
            "agent-template",
            AGENT_TEMPLATE_OUTBOX_TOPIC,
            &key,
            &event_bytes,
            &headers,
            template_domain_result(&stable_result)?,
            committed_version,
            admitted.created_at_ms,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let record = match self.mutations.finish_with_replay(
            &txn,
            &batch,
            Some(result_bytes),
            admitted.created_at_ms,
            Some(source_version),
            (operation, nonce, &receipt),
        ) {
            Ok(record) => record,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let recorded_version = record
            .committed_version
            .target()
            .ok_or_else(|| "agent template commit has no target version".to_string())?;
        if recorded_version != committed_version {
            txn.abort()?;
            return Err("agent template result version differs from the committed version".to_string());
        }
        self.mutations.commit(txn, &batch)?;
        Ok(AgentTemplateWriteResult {
            result: stable_result,
            replayed: false,
        })
    }
}

/// Head CAS plus the append-only revision row, in the template tables.
fn apply_template_rows(
    owner_write: &AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    expected_revision: u64,
    entry: &AgentTemplateEntry,
    entry_bytes: &[u8],
) -> Result<(), String> {
    let mut heads = owner_write.open_table(eg_storage::AGENT_TEMPLATE_HEADS)?;
    let actual_revision = heads
        .get((context.tenant_id.as_str(), entry.template_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    require_expected_revision(expected_revision, actual_revision)?;
    if entry.entry_revision != next_revision(expected_revision)? {
        return Err("agent template entry revision does not follow its expected head".to_string());
    }
    let mut revisions = owner_write.open_table(eg_storage::AGENT_TEMPLATE_REVISIONS)?;
    if actual_revision > 0 {
        let current = revisions
            .get((
                context.tenant_id.as_str(),
                entry.template_id.as_str(),
                actual_revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent template head points to a missing revision".to_string())?;
        let current = decode_template(current.value())?;
        if current.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("retired agent templates cannot be resurrected".to_string());
        }
    }
    if revisions
        .get((
            context.tenant_id.as_str(),
            entry.template_id.as_str(),
            entry.entry_revision,
        ))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("agent template revision already exists".to_string());
    }
    revisions
        .insert(
            (
                context.tenant_id.as_str(),
                entry.template_id.as_str(),
                entry.entry_revision,
            ),
            entry_bytes,
        )
        .map_err(|error| error.to_string())?;
    heads
        .insert(
            (context.tenant_id.as_str(), entry.template_id.as_str()),
            entry.entry_revision,
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn template_operations(
    kind: AgentTemplateMutationKind,
    entry: &AgentTemplateEntry,
) -> Vec<MutationOperation> {
    let event_type = match kind {
        AgentTemplateMutationKind::Publish => "agent_template_publish",
        AgentTemplateMutationKind::Retire => "agent_template_retire",
    };
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: entry.definition_digest.clone(),
        },
    }]
}

fn template_outbox_headers(entry: &AgentTemplateEntry) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_TEMPLATE_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), entry.tenant_id.clone()),
        ("template_id".to_string(), entry.template_id.clone()),
        (
            "entry_revision".to_string(),
            entry.entry_revision.to_string(),
        ),
        ("definition_digest".to_string(), entry.definition_digest.clone()),
        ("definition_actor_scope".to_string(), entry.actor_scope.clone()),
        ("definition_purpose_id".to_string(), entry.purpose_id.clone()),
        (
            "definition_policy_digest".to_string(),
            entry.policy_digest.clone(),
        ),
    ])
}

fn build_template_batch(
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind: AgentTemplateMutationKind,
    entry: &AgentTemplateEntry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let key = format!(
        "{}:{}:{}",
        entry.tenant_id, entry.template_id, entry.entry_revision
    );
    let operations = template_operations(kind, entry);
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_TEMPLATE_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers: template_outbox_headers(entry),
    }];
    // The envelope is minted from the batch's FINAL operations and outbox --
    // `MutationBatch::validate` compares its canonical payload digest against
    // exactly these, so building it from anything earlier fails closed with
    // "mutation batch content does not match its envelope's canonical payload
    // digest".
    let content = BatchContent {
        operations: &operations,
        outbox: &outbox,
        authoritative_state: None,
    };
    let method_schema_digest = eg_capabilities::method_schema("ApplyMutation")
        .map(|(_, digest)| eg_types::contract::Digest256::from_bytes(digest))
        .ok_or_else(|| "ApplyMutation is missing from the contract catalog".to_string())?;
    let compiled = CompiledOperation::for_content(owner.identity(), content, method_schema_digest)?;
    let mut compiled_envelope = CompiledEnvelope::new(
        CompiledScope {
            identity: owner.identity(),
            actor: &context.caller_principal,
            serving_principal: owner.principal(),
            request_id: context.request_id,
            idempotency_key: context.idempotency_key.as_str(),
            nonce: context.attempt_nonce,
            now_ms: context.created_at_ms,
        },
        compiled,
    )?;
    compiled_envelope.catalog_digest =
        eg_types::contract::Digest256::parse(eg_capabilities::CONTRACT_CATALOG_DIGEST)?;
    compiled_envelope.policy_digest =
        super::agent_library::parse_prefixed_digest(&context.policy_digest)?;
    compiled_envelope.policy_revision = context.policy_revision.clone();
    compiled_envelope.policy_decision_id = context.policy_decision_id.clone();
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        envelope: MutationEnvelope::for_compiled_batch(compiled_envelope)?,
        identity: owner.identity().clone(),
        placement_epoch: 0,
        version_expectation: VersionExpectation::Native(version),
        fencing_token: None,
        authoritative_state: None,
        operations,
        outbox,
        created_at_ms: context.created_at_ms,
    };
    batch.validate_write_budget()?;
    Ok(batch)
}

fn read_template_history(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    template_id: &str,
) -> Result<(Option<u64>, Vec<AgentTemplateEntry>), String> {
    let head = read
        .open_owner_table(eg_storage::AGENT_TEMPLATE_HEADS)?
        .get((tenant_id, template_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value());
    let table = read.open_owner_table(eg_storage::AGENT_TEMPLATE_REVISIONS)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, template_id, 0)..=(tenant_id, template_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        // Bounded: a caller can otherwise ask for an unbounded amount of work
        // by publishing revisions.
        if entries.len() >= MAX_AGENT_TEMPLATE_REVISIONS {
            return Err("agent template history exceeds its retained revision bound".to_string());
        }
        bytes = bytes.saturating_add(value.value().len());
        if bytes > MAX_AGENT_TEMPLATE_HISTORY_BYTES {
            return Err("agent template history exceeds its retained byte bound".to_string());
        }
        let (row_tenant, row_template, row_revision) = key.value();
        let entry = decode_template(value.value())?;
        if entry.tenant_id != row_tenant
            || entry.template_id != row_template
            || entry.entry_revision != row_revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent template row does not match its physical key"
                    .to_string(),
            );
        }
        entries.push(entry);
    }
    Ok((head, entries))
}

fn decode_template(bytes: &[u8]) -> Result<AgentTemplateEntry, String> {
    let entry = eg_types::msgpack::decode_bounded::<AgentTemplateEntry>(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AGENT_TEMPLATE_ROW_BYTES,
            MAX_AGENT_TEMPLATE_ROW_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "agent template row is invalid or exceeds resource limits".to_string())?;
    entry.validate()?;
    Ok(entry)
}

fn decode_committed_result(bytes: &[u8]) -> Result<AgentTemplateCommittedResult, String> {
    let result = eg_types::msgpack::decode_bounded::<AgentTemplateCommittedResult>(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AGENT_TEMPLATE_ROW_BYTES,
            MAX_AGENT_TEMPLATE_ROW_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "agent template result is invalid or exceeds resource limits".to_string())?;
    result.template.validate()?;
    Ok(result)
}

fn template_domain_result(result: &AgentTemplateCommittedResult) -> Result<MutationResult, String> {
    let payload = eg_storage::encode_bounded(result, "agent template domain result payload")?;
    let payload = eg_types::contract::RecordBytes::new(payload)?;
    Ok(MutationResult::DomainResult {
        schema_id: eg_types::contract::SchemaId::new(AGENT_TEMPLATE_RESULT_SCHEMA_ID)?,
        payload_digest: payload.digest()?,
        payload,
    })
}

fn encode_template_domain_result(result: &AgentTemplateCommittedResult) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(&template_domain_result(result)?, "agent template domain result")
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
fn replayed_template(
    replay: ReplayResolution,
    template_id: &str,
) -> Result<Option<AgentTemplateWriteResult>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different agent template mutation"
                    .to_string(),
            );
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template replay is missing its typed receipt".to_string(),
        );
    };
    let receipt = *receipt;
    receipt.validate()?;
    let MutationResult::DomainResult { payload, .. } = &receipt.result else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template replay receipt carries no domain result"
                .to_string(),
        );
    };
    let committed = decode_committed_result(payload.as_slice())?;
    if committed.template.template_id != template_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template replay resolved a different template"
                .to_string(),
        );
    }
    Ok(Some(AgentTemplateWriteResult {
        result: committed,
        replayed: true,
    }))
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the template surface.
pub type AgentTemplateStoreRef = Arc<AgentLibraryStore>;


#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
    use eg_types::agent_library::AgentLibraryEntryDraft;
    use eg_types::agent_template::{
        AgentTemplateDraft, AgentTemplateInstantiateRequest, TemplateParam,
    };
    use eg_types::contract::Nonce;

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn dep(component_id: &str, kind: AgentComponentKind, seed: char) -> ComponentDependency {
        ComponentDependency {
            component_id: component_id.to_string(),
            kind,
            definition_digest: digest(seed),
        }
    }

    fn base(tenant_id: &str, agent_id: &str) -> AgentLibraryEntryDraft {
        AgentLibraryEntryDraft {
            agent_id: agent_id.to_string(),
            package_id: "agent-package".to_string(),
            version: "1.0.0".to_string(),
            role: "researcher".to_string(),
            role_digest: digest('1'),
            system_prompt: dep("prompt:agent-v1", AgentComponentKind::SystemPrompt, '2'),
            tools: vec![dep("tool:search", AgentComponentKind::Tool, '3')],
            skills: vec![dep("skill:reason", AgentComponentKind::Skill, '4')],
            model_profile: dep("model-profile:opus", AgentComponentKind::ModelProfile, '5'),
            model_identity: "model:opus".to_string(),
            ontologies: vec![dep("ontology:agent", AgentComponentKind::Ontology, '6')],
            tenant_id: tenant_id.to_string(),
            actor_scope: "definition:builder-a".to_string(),
            purpose_id: "agent-library:definition".to_string(),
            policy_digest: digest('7'),
            source_revision: "source-revision:42".to_string(),
            source_revision_digest: digest('8'),
            runtime: Default::default(),
            instantiated_from: None,
        }
    }

    fn template(tenant_id: &str, template_id: &str) -> AgentTemplateDraft {
        AgentTemplateDraft {
            template_id: template_id.to_string(),
            version: "1.0.0".to_string(),
            base: base(tenant_id, "agent:researcher"),
            params: vec![
                TemplateParam {
                    name: "model".to_string(),
                    replaces: "model-profile:opus".to_string(),
                    kind: AgentComponentKind::ModelProfile,
                    required: false,
                    summary: "which model the agent runs on".to_string(),
                },
                TemplateParam {
                    name: "search".to_string(),
                    replaces: "tool:search".to_string(),
                    kind: AgentComponentKind::Tool,
                    required: false,
                    summary: "which search tool the agent uses".to_string(),
                },
            ],
            tenant_id: tenant_id.to_string(),
            actor_scope: "definition:builder-a".to_string(),
            purpose_id: "agent-template:definition".to_string(),
            policy_digest: digest('9'),
        }
    }

    fn context(
        store: &AgentLibraryStore,
        tenant_id: &str,
        key: &str,
        nonce: u8,
        expected_revision: u64,
        purpose_id: &str,
    ) -> AgentLibraryMutationContext {
        AgentLibraryMutationContext {
            request_id: u64::from(nonce),
            principal: store.owner_principal().to_string(),
            caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
            attempt_nonce: Nonce::from_bytes([nonce; 32]),
            tenant_id: tenant_id.to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: purpose_id.to_string(),
            policy_revision: "policy-v1".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            policy_decision_id: "agent-template:decision:policy-v1".to_string(),
            idempotency_key: key.to_string(),
            expected_revision: Some(expected_revision),
            trace_id: None,
            created_at_ms: 10,
        }
    }

    fn open_store() -> (tempfile::TempDir, AgentLibraryStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        (dir, store)
    }

    fn publish(
        store: &AgentLibraryStore,
        key: &str,
        nonce: u8,
        expected_revision: u64,
        draft: AgentTemplateDraft,
    ) -> AgentTemplateWriteResult {
        store
            .publish_template(AgentTemplatePublishRequest {
                context: context(
                    store,
                    &draft.tenant_id.clone(),
                    key,
                    nonce,
                    expected_revision,
                    "agent-template:publish",
                ),
                template: draft,
            })
            .unwrap()
    }

    fn instantiate(
        tenant_id: &str,
        template_id: &str,
        agent_id: &str,
        entry_revision: Option<u64>,
        bindings: BTreeMap<String, ComponentDependency>,
    ) -> AgentTemplateInstantiateRequest {
        AgentTemplateInstantiateRequest {
            tenant_id: tenant_id.to_string(),
            template_id: template_id.to_string(),
            entry_revision,
            agent_id: agent_id.to_string(),
            bindings,
        }
    }

    #[test]
    fn a_published_template_is_durable_and_reads_back() {
        let (_dir, store) = open_store();
        let published = publish(&store, "key-1", 1, 0, template("tenant-a", "template:researcher"));
        assert!(!published.replayed);
        assert_eq!(published.result.template.entry_revision, 1);

        let current = store
            .current_template("tenant-a", "template:researcher")
            .unwrap()
            .unwrap();
        assert_eq!(current, published.result.template);
    }

    #[test]
    fn a_byte_identical_retry_replays_rather_than_publishing_twice() {
        let (_dir, store) = open_store();
        let first = publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
        let mut retry = context(&store, "tenant-a", "key-1", 2, 0, "agent-template:publish");
        retry.created_at_ms = 99;
        let replayed = store
            .publish_template(AgentTemplatePublishRequest {
                context: retry,
                template: template("tenant-a", "template:a"),
            })
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, first.result);
        assert_eq!(
            store.template_revisions("tenant-a", "template:a").unwrap().len(),
            1
        );
    }

    #[test]
    fn the_four_layers_share_one_owner_without_colliding() {
        // Components, agents, graphs and templates all live in
        // `agent_library.redb`. If any pair shared a key space, publishing one
        // would overwrite another -- and a template's base is literally an
        // agent draft, so a collision here would be easy to miss.
        let (_dir, store) = open_store();
        publish(&store, "key-1", 1, 0, template("tenant-a", "same-id"));
        assert!(store.current_template("tenant-a", "same-id").unwrap().is_some());
        assert!(store.current("tenant-a", "same-id").unwrap().is_none());
        assert!(store.current_graph("tenant-a", "same-id").unwrap().is_none());
        assert!(store
            .current_component("tenant-a", "same-id")
            .unwrap()
            .is_none());
    }

    #[test]
    fn the_store_reopens_after_a_template_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            let store = AgentLibraryStore::open(path).unwrap();
            publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
        }
        let reopened = AgentLibraryStore::open(path).expect("owner reopens");
        assert!(reopened
            .current_template("tenant-a", "template:a")
            .unwrap()
            .is_some());
    }

    #[test]
    fn a_history_read_is_scoped_to_its_own_template() {
        // `range_from` is open-ended. Without a per-row prefix re-check a
        // history read walks into the NEXT template's revisions and returns
        // them as if they belonged to this one.
        let (_dir, store) = open_store();
        publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
        publish(&store, "key-2", 2, 0, template("tenant-a", "template:b"));
        publish(&store, "key-3", 3, 0, template("tenant-b", "template:a"));
        for (tenant, id) in [
            ("tenant-a", "template:a"),
            ("tenant-a", "template:b"),
            ("tenant-b", "template:a"),
        ] {
            let revisions = store.template_revisions(tenant, id).unwrap();
            assert_eq!(revisions.len(), 1, "{tenant}/{id}");
            assert_eq!(revisions[0].tenant_id, tenant);
            assert_eq!(revisions[0].template_id, id);
        }
    }

    // ---- the operation the layer exists for ----

    #[test]
    fn an_instance_is_an_ordinary_library_entry_and_publishes_like_one() {
        // THE property: instantiation yields a normal agent draft, so the
        // instance is admitted, delegated and pinned with no template-aware
        // branch anywhere. The only thing it carries is its provenance.
        let (_dir, store) = open_store();
        // The instance publishes through the ordinary library path, which now
        // RESOLVES every pinned component against the durable store -- so the
        // template's base and the binding have to name components that really
        // exist, exactly as a real template does.
        let mut template = template("tenant-a", "template:researcher");
        super::super::agent_component::seed_draft_components_for_test(&store, &mut template.base, 30);
        let haiku = super::super::agent_component::seed_component_for_test(
            &store,
            "tenant-a",
            "model-profile:haiku",
            AgentComponentKind::ModelProfile,
            40,
        );
        publish(&store, "key-1", 1, 0, template);
        let bindings = BTreeMap::from([("model".to_string(), haiku)]);
        let draft = store
            .instantiate_template(&instantiate(
                "tenant-a",
                "template:researcher",
                "agent:researcher-cheap",
                None,
                bindings,
            ))
            .expect("instantiates");
        assert_eq!(draft.model_profile.component_id, "model-profile:haiku");
        // Unbound optional parameter: the base component stands.
        assert_eq!(draft.tools[0].component_id, "tool:search");
        let provenance = draft
            .instantiated_from
            .clone()
            .expect("an instance records where it came from");
        assert_eq!(provenance.template_id, "template:researcher");
        assert_eq!(provenance.entry_revision, 1);

        // And it publishes through the ordinary library path, unchanged.
        let published = store
            .publish(eg_types::agent_library::AgentLibraryPublishRequest {
                context: context(
                    &store,
                    "tenant-a",
                    "key-2",
                    2,
                    0,
                    "agent-library:definition",
                ),
                entry: draft,
            })
            .expect("an instance publishes like any other agent");
        let stored = store
            .current("tenant-a", "agent:researcher-cheap")
            .unwrap()
            .unwrap();
        assert_eq!(stored.entry_revision, published.entry.entry_revision);
        assert_eq!(stored.instantiated_from, Some(provenance));
    }

    #[test]
    fn a_retired_template_stops_instantiating_at_every_revision_it_ever_had() {
        // The lifecycle consulted is the HEAD's. A tombstone is a separate
        // LATER revision, so revision 1 stays `Published` in its own row
        // forever -- resolving the pinned revision's lifecycle instead would
        // let a withdrawn template keep minting agents indefinitely.
        let (_dir, store) = open_store();
        publish(&store, "key-1", 1, 0, template("tenant-a", "template:researcher"));
        assert!(store
            .instantiate_template(&instantiate(
                "tenant-a",
                "template:researcher",
                "agent:a",
                Some(1),
                BTreeMap::new(),
            ))
            .is_ok());

        store
            .retire_template(AgentTemplateRetireRequest {
                context: context(&store, "tenant-a", "key-2", 2, 1, "agent-template:retire"),
                template_id: "template:researcher".to_string(),
            })
            .unwrap();

        for pinned in [None, Some(1), Some(2)] {
            let error = store
                .instantiate_template(&instantiate(
                    "tenant-a",
                    "template:researcher",
                    "agent:a",
                    pinned,
                    BTreeMap::new(),
                ))
                .expect_err("a retired template must instantiate nothing");
            assert!(error.contains("retired"), "pinned={pinned:?}: {error}");
        }
        // Retiring withdraws it; it stays readable, because an agent that
        // already recorded it as its provenance still needs to resolve it.
        assert_eq!(
            store
                .template_revisions("tenant-a", "template:researcher")
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn an_instantiate_cannot_reach_another_tenants_template() {
        let (_dir, store) = open_store();
        publish(&store, "key-1", 1, 0, template("tenant-a", "template:researcher"));
        let error = store
            .instantiate_template(&instantiate(
                "tenant-b",
                "template:researcher",
                "agent:a",
                None,
                BTreeMap::new(),
            ))
            .expect_err("another tenant's template must not resolve");
        assert!(error.contains("no such agent template"), "got: {error}");
    }

    #[test]
    fn an_instantiate_of_a_revision_that_was_never_retained_is_refused() {
        let (_dir, store) = open_store();
        publish(&store, "key-1", 1, 0, template("tenant-a", "template:researcher"));
        let error = store
            .instantiate_template(&instantiate(
                "tenant-a",
                "template:researcher",
                "agent:a",
                Some(7),
                BTreeMap::new(),
            ))
            .expect_err("an unretained revision must be refused");
        assert!(error.contains("no such retained revision"), "got: {error}");
    }

    #[test]
    fn an_unknown_binding_is_refused_by_the_store_too() {
        // The type layer refuses it; this proves the store path does not
        // bypass that check on its way through.
        let (_dir, store) = open_store();
        publish(&store, "key-1", 1, 0, template("tenant-a", "template:researcher"));
        let bindings = BTreeMap::from([(
            "temperature".to_string(),
            dep("model-profile:haiku", AgentComponentKind::ModelProfile, 'b'),
        )]);
        let error = store
            .instantiate_template(&instantiate(
                "tenant-a",
                "template:researcher",
                "agent:a",
                None,
                bindings,
            ))
            .expect_err("an undeclared parameter must be refused");
        assert!(error.contains("declares no parameter"), "got: {error}");
    }
}
