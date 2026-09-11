//! Durable revisions for agent COMPONENTS -- layer 1 of the agent hierarchy
//! (RF-ADR-008).
//!
//! Model profiles, prompts, tools, MCP servers/prompts/resources, skills,
//! schemas and predicates: the parts that [`super::agent_library`] entries are
//! assembled from and [`super::agent_graph`] graphs compose. Published into the
//! SAME owner file as both, because a question about an agent system should be
//! answerable from one place rather than by reconciling three
//! (RF-RULING-004).
//!
//! Source packages and MCP servers remain the ORIGIN of these; EG is where they
//! are recorded so they can be queried. `ComponentProvenance` on each record is
//! what makes that split work -- without it a re-ingest cannot tell an updated
//! component from a new one, and "which agents break if this server changes?"
//! has nothing to traverse.
//!
//! The revision protocol is the same one the other two layers use, and reuses
//! the same machinery from [`super::agent_library`]. See that module's notes on
//! why the shared parts are shared by parameter rather than by a trait, and why
//! generalizing is safer once there are several tested implementations than
//! before there is one.

use std::collections::BTreeMap;
use std::sync::Arc;

use redb::ReadableTable;

use eg_storage::{OwnedStoreHandle, RecordedOperation, ScopedRead};
use eg_transaction::{AdmittedOwnerWrite, Begin, ReplayResolution};

use eg_types::agent_component::{
    AgentComponentCommittedResult, AgentComponentEntry, AgentComponentMutationKind,
    AgentComponentOutboxEvent, AgentComponentPublishRequest, AgentComponentRetireRequest,
    AgentComponentStatusRequest, AGENT_COMPONENT_SCHEMA_VERSION,
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

const AGENT_COMPONENT_OUTBOX_TOPIC: &str = "eg.agent-component.revision.v1";
const AGENT_COMPONENT_RESULT_SCHEMA_ID: &str = "agent-component-result.v1";
const MAX_AGENT_COMPONENT_ROW_BYTES: usize = 16 * 1024 * 1024;
const MAX_AGENT_COMPONENT_ROW_ITEMS: usize = 200_000;
const MAX_AGENT_COMPONENT_REVISIONS: usize = 16_384;
const MAX_AGENT_COMPONENT_HISTORY_BYTES: usize = 256 * 1024 * 1024;

/// What a committed graph write returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentComponentWriteResult {
    pub result: AgentComponentCommittedResult,
    pub replayed: bool,
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
                "agent component publish context tenant does not match the graph's tenant".to_string(),
            );
        }
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent component writes require an explicit expected_revision".to_string())?;
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
        let replay_context =
            match admitted_context(&request.context, "agent-component:publish") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "component-publish",
            &request.component.component_id,
            expected_revision,
            Some(&request.component.content_digest),
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
        if let Some(result) = match replayed_component(replay, &request.component.component_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        let entry = match AgentComponentEntry::create(
            request.component,
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
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent component writes require an explicit expected_revision".to_string())?;
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
            match admitted_context(&request.context, "agent-component:retire") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "component-retire",
            &request.component_id,
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
        if let Some(result) = match replayed_component(replay, &request.component_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        let current = match self.component_at_revision_in_write(
            &txn,
            &request.context.tenant_id,
            &request.component_id,
            expected_revision,
        ) {
            Ok(Some(current)) => current,
            Ok(None) => {
                txn.abort()?;
                return Err("agent component has no revision to retire".to_string());
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

    /// The head revision of one graph, or `None` if it was never published.
    pub fn current_component(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<Option<AgentComponentEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, component_id)?;
        let read = self.read()?;
        Ok(read_component_history(&read, tenant_id, component_id)?
            .1
            .into_iter()
            .last())
    }

    /// Every retained revision of one graph, oldest first.
    pub fn component_revisions(
        &self,
        tenant_id: &str,
        component_id: &str,
    ) -> Result<Vec<AgentComponentEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, component_id)?;
        let read = self.read()?;
        Ok(read_component_history(&read, tenant_id, component_id)?.1)
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn component_status(
        &self,
        request: AgentComponentStatusRequest,
    ) -> Result<Option<AgentComponentWriteResult>, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.component_id)?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let batch_id = batch_id(&request.context.idempotency_key)?;
        let read = self.kernel.read_scope(&owner)?;
        let Some(record) = eg_transaction::read_ledger(&read, &batch_id)? else {
            return Ok(None);
        };
        let Some(result_bytes) = record.result_msgpack.as_ref() else {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component status has no typed result".to_string(),
            );
        };
        let committed = decode_committed_result(result_bytes)?;
        if committed.component.component_id != request.component_id {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component status resolved a different graph"
                    .to_string(),
            );
        }
        Ok(Some(AgentComponentWriteResult {
            result: committed,
            replayed: true,
        }))
    }

    /// Find the published components that answer a capability search.
    ///
    /// The durable half of *"what does an agent trying to do XYZ need?"*: the
    /// request resolves a task to capabilities through EG's native ontology,
    /// and each component's classification is matched by subsumption.
    ///
    /// Only HEAD revisions are considered, and only published ones. A search is
    /// asking "what could I build with today", so a superseded revision or a
    /// withdrawn component is not an answer -- while both remain readable by
    /// id, because an existing agent that pinned one still needs to resolve it.
    pub fn search_components(
        &self,
        request: &eg_types::agent_component::AgentComponentSearchRequest,
    ) -> Result<Vec<AgentComponentEntry>, String> {
        request.validate()?;
        let read = self.read()?;
        let heads = read.open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?;
        let revisions = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
        let mut matched = Vec::new();
        let mut scanned = 0usize;
        for row in heads
            .range((request.tenant_id.as_str(), "")..)
            .map_err(|error| error.to_string())?
        {
            scanned += 1;
            if scanned > MAX_AGENT_COMPONENT_REVISIONS {
                return Err("agent component search exceeds its scan bound".to_string());
            }
            let (key, head_revision) = row.map_err(|error| error.to_string())?;
            let (row_tenant, component_id) = key.value();
            // `range_from` is open-ended: without this the scan walks into the
            // NEXT tenant's components and would return them.
            if row_tenant != request.tenant_id {
                break;
            }
            let Some(value) = revisions
                .get((row_tenant, component_id, head_revision.value()))
                .map_err(|error| error.to_string())?
            else {
                return Err("agent component head points to a missing revision".to_string());
            };
            let entry = decode_component(value.value())?;
            if request.matches(&entry) {
                matched.push(entry);
            }
        }
        Ok(matched)
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
        let operations = component_operations(kind, &entry);
        let policy_digest = effective_agent_library_policy_digest(&operations)?;
        let mut admitted = context.clone();
        admitted.policy_digest = format!("sha256:{}", policy_digest.to_hex());

        let event = AgentComponentOutboxEvent {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            kind,
            component: entry.clone(),
            performing_actor: admitted.caller_principal.clone(),
            action_actor_scope: admitted.actor_scope.clone(),
        };
        event.validate()?;
        let event_bytes = eg_storage::encode_bounded(&event, "agent component outbox event")?;
        let entry_bytes = eg_storage::encode_bounded(&entry, "agent component revision")?;
        let batch_id = batch_id(&admitted.idempotency_key)?;

        let authoritative_version = match self.mutations.current_version(&txn, owner) {
            Ok(version) => version,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let batch = match build_component_batch(
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
                return Err("agent component admission has no native source version".to_string());
            }
            Begin::Replay(_) => {
                txn.abort()?;
                return Err("CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision".to_string());
            }
        };
        if source_version != authoritative_version {
            txn.abort()?;
            return Err("agent component source version changed while admitting write".to_string());
        }
        let committed_version = match source_version.checked_add(1) {
            Some(version) => version,
            None => {
                txn.abort()?;
                return Err("agent component committed version overflow".to_string());
            }
        };
        let stable_result = AgentComponentCommittedResult {
            schema_version: AGENT_COMPONENT_SCHEMA_VERSION,
            component: entry.clone(),
            batch_id: batch_id.clone(),
            committed_version,
        };
        let result_bytes = match encode_component_domain_result(&stable_result) {
            Ok(bytes) => bytes,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };

        let owner_write_result: Result<(), String> = (|| {
            let owner_write = txn.owner_rows(owner, &batch)?;
            apply_component_rows(
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
            entry.tenant_id, entry.component_id, entry.entry_revision
        );
        let headers = component_outbox_headers(&entry);
        let receipt = match owner_receipt(
            operation,
            nonce,
            &batch,
            "agent-component",
            AGENT_COMPONENT_OUTBOX_TOPIC,
            &key,
            &event_bytes,
            &headers,
            component_domain_result(&stable_result)?,
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
            .ok_or_else(|| "agent component commit has no target version".to_string())?;
        if recorded_version != committed_version {
            txn.abort()?;
            return Err("agent component result version differs from the committed version".to_string());
        }
        self.mutations.commit(txn, &batch)?;
        Ok(AgentComponentWriteResult {
            result: stable_result,
            replayed: false,
        })
    }
}

/// Head CAS plus the append-only revision row, in the graph tables.
fn apply_component_rows(
    owner_write: &AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    expected_revision: u64,
    entry: &AgentComponentEntry,
    entry_bytes: &[u8],
) -> Result<(), String> {
    let mut heads = owner_write.open_table(eg_storage::AGENT_COMPONENT_HEADS)?;
    let actual_revision = heads
        .get((context.tenant_id.as_str(), entry.component_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    require_expected_revision(expected_revision, actual_revision)?;
    if entry.entry_revision != next_revision(expected_revision)? {
        return Err("agent component entry revision does not follow its expected head".to_string());
    }
    let mut revisions = owner_write.open_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    if actual_revision > 0 {
        let current = revisions
            .get((
                context.tenant_id.as_str(),
                entry.component_id.as_str(),
                actual_revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent component head points to a missing revision".to_string())?;
        let current = decode_component(current.value())?;
        if current.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("retired agent components cannot be resurrected".to_string());
        }
    }
    if revisions
        .get((
            context.tenant_id.as_str(),
            entry.component_id.as_str(),
            entry.entry_revision,
        ))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("agent component revision already exists".to_string());
    }
    revisions
        .insert(
            (
                context.tenant_id.as_str(),
                entry.component_id.as_str(),
                entry.entry_revision,
            ),
            entry_bytes,
        )
        .map_err(|error| error.to_string())?;
    heads
        .insert(
            (context.tenant_id.as_str(), entry.component_id.as_str()),
            entry.entry_revision,
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn component_operations(
    kind: AgentComponentMutationKind,
    entry: &AgentComponentEntry,
) -> Vec<MutationOperation> {
    let event_type = match kind {
        AgentComponentMutationKind::Publish => "agent_component_publish",
        AgentComponentMutationKind::Retire => "agent_component_retire",
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

fn component_outbox_headers(entry: &AgentComponentEntry) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_COMPONENT_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), entry.tenant_id.clone()),
        ("component_id".to_string(), entry.component_id.clone()),
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

fn build_component_batch(
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind: AgentComponentMutationKind,
    entry: &AgentComponentEntry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let key = format!(
        "{}:{}:{}",
        entry.tenant_id, entry.component_id, entry.entry_revision
    );
    let operations = component_operations(kind, entry);
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_COMPONENT_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers: component_outbox_headers(entry),
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

fn read_component_history(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    component_id: &str,
) -> Result<(Option<u64>, Vec<AgentComponentEntry>), String> {
    let head = read
        .open_owner_table(eg_storage::AGENT_COMPONENT_HEADS)?
        .get((tenant_id, component_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value());
    let table = read.open_owner_table(eg_storage::AGENT_COMPONENT_REVISIONS)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, component_id, 0)..=(tenant_id, component_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        // Bounded: a caller can otherwise ask for an unbounded amount of work
        // by publishing revisions.
        if entries.len() >= MAX_AGENT_COMPONENT_REVISIONS {
            return Err("agent component history exceeds its retained revision bound".to_string());
        }
        bytes = bytes.saturating_add(value.value().len());
        if bytes > MAX_AGENT_COMPONENT_HISTORY_BYTES {
            return Err("agent component history exceeds its retained byte bound".to_string());
        }
        let (row_tenant, row_graph, row_revision) = key.value();
        let entry = decode_component(value.value())?;
        if entry.tenant_id != row_tenant
            || entry.component_id != row_graph
            || entry.entry_revision != row_revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent component row does not match its physical key"
                    .to_string(),
            );
        }
        entries.push(entry);
    }
    Ok((head, entries))
}

fn decode_component(bytes: &[u8]) -> Result<AgentComponentEntry, String> {
    let entry = eg_types::msgpack::decode_bounded::<AgentComponentEntry>(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AGENT_COMPONENT_ROW_BYTES,
            MAX_AGENT_COMPONENT_ROW_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "agent component row is invalid or exceeds resource limits".to_string())?;
    entry.validate()?;
    Ok(entry)
}

fn decode_committed_result(bytes: &[u8]) -> Result<AgentComponentCommittedResult, String> {
    let result = eg_types::msgpack::decode_bounded::<AgentComponentCommittedResult>(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AGENT_COMPONENT_ROW_BYTES,
            MAX_AGENT_COMPONENT_ROW_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "agent component result is invalid or exceeds resource limits".to_string())?;
    result.component.validate()?;
    Ok(result)
}

fn component_domain_result(result: &AgentComponentCommittedResult) -> Result<MutationResult, String> {
    let payload = eg_storage::encode_bounded(result, "agent component domain result payload")?;
    let payload = eg_types::contract::RecordBytes::new(payload)?;
    Ok(MutationResult::DomainResult {
        schema_id: eg_types::contract::SchemaId::new(AGENT_COMPONENT_RESULT_SCHEMA_ID)?,
        payload_digest: payload.digest()?,
        payload,
    })
}

fn encode_component_domain_result(result: &AgentComponentCommittedResult) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(&component_domain_result(result)?, "agent component domain result")
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
fn replayed_component(
    replay: ReplayResolution,
    component_id: &str,
) -> Result<Option<AgentComponentWriteResult>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different agent component mutation"
                    .to_string(),
            );
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent component replay is missing its typed receipt".to_string(),
        );
    };
    let receipt = *receipt;
    receipt.validate()?;
    let MutationResult::DomainResult { payload, .. } = &receipt.result else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent component replay receipt carries no domain result"
                .to_string(),
        );
    };
    let committed = decode_committed_result(payload.as_slice())?;
    if committed.component.component_id != component_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent component replay resolved a different graph".to_string(),
        );
    }
    Ok(Some(AgentComponentWriteResult {
        result: committed,
        replayed: true,
    }))
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the graph surface.
pub type AgentComponentStoreRef = Arc<AgentLibraryStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_component::{
        AgentComponentDraft, AgentComponentFacts, AgentComponentKind, AgentComponentSearchRequest,
        ComponentProvenance, ToolEffect,
    };
    use eg_types::contract::Nonce;

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn tool(component_id: &str, capability: &str, effect: ToolEffect) -> AgentComponentDraft {
        AgentComponentDraft {
            component_id: component_id.to_string(),
            kind: AgentComponentKind::Tool,
            version: "1.0.0".to_string(),
            content_digest: digest('1'),
            content_ref: None,
            facts: AgentComponentFacts::Tool {
                effect,
                required_scopes: Vec::new(),
            },
            provenance: ComponentProvenance::McpServer {
                server_component_id: "mcp:search-server".to_string(),
                upstream_name: component_id.to_string(),
            },
            summary: format!("tool {component_id}"),
            classification: vec![capability.to_string()],
            requires: Vec::new(),
            provides: Vec::new(),
            attributes: Default::default(),
            tenant_id: "tenant-a".to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: "agent-component:publish".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            source_revision: "rev-1".to_string(),
            source_revision_digest: digest('8'),
        }
    }

    fn context(
        store: &AgentLibraryStore,
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
            tenant_id: "tenant-a".to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: purpose_id.to_string(),
            policy_revision: "policy-v1".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            policy_decision_id: "agent-component:decision:policy-v1".to_string(),
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

    #[test]
    fn a_published_component_is_durable_and_reads_back() {
        let (_dir, store) = open_store();
        let published = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool(
                    "tool:web-search",
                    "eg:capability/retrieval/web-search",
                    ToolEffect::Read,
                ),
            })
            .unwrap();
        assert!(!published.replayed);
        assert_eq!(published.result.component.entry_revision, 1);

        let current = store
            .current_component("tenant-a", "tool:web-search")
            .unwrap()
            .unwrap();
        assert_eq!(current, published.result.component);
    }

    #[test]
    fn a_byte_identical_retry_replays_rather_than_publishing_twice() {
        let (_dir, store) = open_store();
        let first = store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool("tool:a", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        let mut retry = context(&store, "key-1", 2, 0, "agent-component:publish");
        retry.created_at_ms = 99;
        let replayed = store
            .publish_component(AgentComponentPublishRequest {
                context: retry,
                component: tool("tool:a", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, first.result);
        assert_eq!(
            store.component_revisions("tenant-a", "tool:a").unwrap().len(),
            1
        );
    }

    #[test]
    fn the_three_layers_share_one_owner_without_colliding() {
        // Components, agents and graphs all live in `agent_library.redb`. If any
        // pair shared a key space, publishing one would overwrite another.
        let (_dir, store) = open_store();
        store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool("same-id", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        assert!(store.current_component("tenant-a", "same-id").unwrap().is_some());
        assert!(store.current("tenant-a", "same-id").unwrap().is_none());
        assert!(store.current_graph("tenant-a", "same-id").unwrap().is_none());
    }

    #[test]
    fn the_store_reopens_after_a_component_commit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            let store = AgentLibraryStore::open(path).unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                    component: tool(
                        "tool:a",
                        "eg:capability/retrieval/web-search",
                        ToolEffect::Read,
                    ),
                })
                .unwrap();
        }
        let reopened = AgentLibraryStore::open(path).expect("owner reopens");
        assert!(reopened
            .current_component("tenant-a", "tool:a")
            .unwrap()
            .is_some());
    }

    // ---- the query the layer exists for ----

    fn seed_search_corpus(store: &AgentLibraryStore) {
        let corpus = [
            ("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read),
            ("tool:vector", "eg:capability/retrieval/vector-search", ToolEffect::Read),
            ("tool:summarize", "eg:capability/analysis/summarize", ToolEffect::Read),
            ("tool:deploy", "eg:capability/action/process-exec", ToolEffect::Write),
        ];
        for (index, (id, capability, effect)) in corpus.iter().enumerate() {
            let nonce = u8::try_from(index + 1).unwrap();
            store
                .publish_component(AgentComponentPublishRequest {
                    context: context(
                        store,
                        &format!("key-{index}"),
                        nonce,
                        0,
                        "agent-component:publish",
                    ),
                    component: tool(id, capability, *effect),
                })
                .unwrap();
        }
    }

    fn search(tenant: &str, task: Option<&str>, read_only: bool) -> AgentComponentSearchRequest {
        AgentComponentSearchRequest {
            tenant_id: tenant.to_string(),
            task: task.map(str::to_string),
            capabilities: Vec::new(),
            kinds: Vec::new(),
            read_only,
        }
    }

    #[test]
    fn a_task_search_returns_what_an_agent_doing_it_would_need() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let found = store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap();
        let ids: Vec<&str> = found.iter().map(|c| c.component_id.as_str()).collect();
        // Both retrieval tools match the task's general `retrieval` need by
        // subsumption, and so does the summarizer.
        assert!(ids.contains(&"tool:web"), "{ids:?}");
        assert!(ids.contains(&"tool:vector"), "{ids:?}");
        assert!(ids.contains(&"tool:summarize"), "{ids:?}");
        assert!(!ids.contains(&"tool:deploy"), "a deploy tool is not research: {ids:?}");
    }

    #[test]
    fn a_read_only_search_excludes_every_side_effecting_component() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let found = store
            .search_components(&search("tenant-a", Some("eg:task/operate"), true))
            .unwrap();
        assert!(
            found.iter().all(|c| !c.is_side_effecting()),
            "read_only must exclude every write tool"
        );
        // And without the filter the same task DOES surface the write tool --
        // proving the filter is doing work rather than the corpus being empty.
        let unfiltered = store
            .search_components(&search("tenant-a", Some("eg:task/operate"), false))
            .unwrap();
        assert!(unfiltered.iter().any(|c| c.is_side_effecting()));
    }

    #[test]
    fn a_search_is_scoped_to_its_tenant() {
        let (_dir, store) = open_store();
        seed_search_corpus(&store);
        let found = store
            .search_components(&search("tenant-b", Some("eg:task/research"), false))
            .unwrap();
        assert!(found.is_empty(), "another tenant's components must not leak");
    }

    #[test]
    fn a_retired_component_stops_matching_but_stays_readable() {
        // A search asks "what could I build with today", so a withdrawn
        // component is not an answer. It must still resolve by id, because an
        // agent that pinned it needs to.
        let (_dir, store) = open_store();
        store
            .publish_component(AgentComponentPublishRequest {
                context: context(&store, "key-1", 1, 0, "agent-component:publish"),
                component: tool("tool:web", "eg:capability/retrieval/web-search", ToolEffect::Read),
            })
            .unwrap();
        assert_eq!(
            store
                .search_components(&search("tenant-a", Some("eg:task/research"), false))
                .unwrap()
                .len(),
            1
        );
        store
            .retire_component(AgentComponentRetireRequest {
                context: context(&store, "key-2", 2, 1, "agent-component:retire"),
                component_id: "tool:web".to_string(),
            })
            .unwrap();
        assert!(store
            .search_components(&search("tenant-a", Some("eg:task/research"), false))
            .unwrap()
            .is_empty());
        assert!(store
            .current_component("tenant-a", "tool:web")
            .unwrap()
            .is_some());
        assert_eq!(
            store.component_revisions("tenant-a", "tool:web").unwrap().len(),
            2,
            "both the publish and its tombstone are retained"
        );
    }

    #[test]
    fn a_search_with_neither_a_task_nor_a_capability_is_refused() {
        let (_dir, store) = open_store();
        let error = store
            .search_components(&search("tenant-a", None, false))
            .expect_err("an unconstrained search must be refused");
        assert!(error.contains("task or at least one capability"), "got: {error}");
    }
}
