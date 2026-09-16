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

use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::agent_template::{
    AgentTemplateCommittedResult, AgentTemplateEntry, AgentTemplateMutationKind,
    AgentTemplateOutboxEvent, AgentTemplatePublishRequest, AgentTemplateRetireRequest,
    AgentTemplateStatusRequest, AGENT_TEMPLATE_SCHEMA_VERSION,
};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{
    BatchContent, CompiledEnvelope, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationBatch, MutationBatchStatus, MutationEnvelope, MutationOperation, MutationOutboxIntent,
    MutationSurface, VersionExpectation, MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;

use super::agent_library::{
    admitted_context, agent_library_operation_identity, batch_id,
    effective_agent_library_policy_digest, next_revision, owner_receipt, require_expected_revision,
    resolve_nonce_first, validate_context, AgentLibraryStore, OwnerReceiptInput,
};
use super::agent_row;

mod write;

const AGENT_TEMPLATE_OUTBOX_TOPIC: &str = "eg.agent-template.revision.v1";
const AGENT_TEMPLATE_RESULT_SCHEMA_ID: &str = "agent-template-result.v1";
/// `pub(super)` for [`super::agent_pin_resolution`]: the bound on how deep one
/// pin lookup may scan this layer's history is this layer's own.
pub(super) const MAX_AGENT_TEMPLATE_REVISIONS: usize = 16_384;
const MAX_AGENT_TEMPLATE_HISTORY_BYTES: usize = 256 * 1024 * 1024;

/// What a committed template write returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTemplateWriteResult {
    pub result: AgentTemplateCommittedResult,
    pub replayed: bool,
}

impl AgentLibraryStore {
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
        let committed = decode_template_status_result(result_bytes)?;
        validate_template_status_record(
            &record,
            owner.identity(),
            &request.context,
            &request.template_id,
            request.kind,
            &committed,
        )?;
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
        (
            "definition_digest".to_string(),
            entry.definition_digest.clone(),
        ),
        (
            "definition_actor_scope".to_string(),
            entry.actor_scope.clone(),
        ),
        (
            "definition_purpose_id".to_string(),
            entry.purpose_id.clone(),
        ),
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

/// `pub(super)` for [`super::agent_pin_resolution`], which resolves a pin
/// against this layer and therefore has to read this layer's rows.
pub(super) fn decode_template(bytes: &[u8]) -> Result<AgentTemplateEntry, String> {
    let entry: AgentTemplateEntry = agent_row::decode(bytes, "agent template row")?;
    entry.validate()?;
    Ok(entry)
}

fn decode_committed_result(bytes: &[u8]) -> Result<AgentTemplateCommittedResult, String> {
    let result: AgentTemplateCommittedResult = agent_row::decode(bytes, "agent template result")?;
    result.template.validate()?;
    Ok(result)
}

fn decode_template_status_result(bytes: &[u8]) -> Result<AgentTemplateCommittedResult, String> {
    let mutation_result: MutationResult = agent_row::decode(bytes, "agent template status result")?;
    mutation_result.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = mutation_result
    else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template status result is not a DomainResult"
                .to_string(),
        );
    };
    if schema_id.as_str() != AGENT_TEMPLATE_RESULT_SCHEMA_ID {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template status result has an unexpected schema"
                .to_string(),
        );
    }
    decode_committed_result(payload.as_slice())
}

fn validate_template_status_record(
    record: &eg_types::MutationBatchRecord,
    owner_identity: &eg_types::MutationScopeIdentity,
    context: &AgentLibraryMutationContext,
    template_id: &str,
    kind: AgentTemplateMutationKind,
    committed: &AgentTemplateCommittedResult,
) -> Result<(), String> {
    validate_template_status_durable_state(record)?;
    validate_template_status_identity(record, owner_identity, context, committed)?;
    validate_template_status_result(record, template_id, kind, committed)
}

fn validate_template_status_durable_state(
    record: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    if record.status != MutationBatchStatus::Committed {
        return Err("CORRUPT_MUTATION_LEDGER: agent template status is not committed".to_string());
    }
    record.validate()
}

fn validate_template_status_identity(
    record: &eg_types::MutationBatchRecord,
    owner_identity: &eg_types::MutationScopeIdentity,
    context: &AgentLibraryMutationContext,
    committed: &AgentTemplateCommittedResult,
) -> Result<(), String> {
    let expected_batch_id = batch_id(&context.idempotency_key)?;
    if record.identity != *owner_identity
        || record.batch.batch_id != expected_batch_id
        || committed.batch_id != expected_batch_id
        || record.batch.batch_id != committed.batch_id
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template status batch identity is invalid".to_string(),
        );
    }
    if record.committing_tenant()? != context.tenant_id
        || committed.template.tenant_id != context.tenant_id
    {
        return Err("CORRUPT_MUTATION_LEDGER: agent template status tenant is invalid".to_string());
    }
    Ok(())
}

fn validate_template_status_result(
    record: &eg_types::MutationBatchRecord,
    template_id: &str,
    kind: AgentTemplateMutationKind,
    committed: &AgentTemplateCommittedResult,
) -> Result<(), String> {
    if record.committed_version.target() != Some(committed.committed_version) {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template status version is invalid".to_string(),
        );
    }
    if committed.template.template_id != template_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template status resolved a different template"
                .to_string(),
        );
    }
    let expected_lifecycle = match kind {
        AgentTemplateMutationKind::Publish => AgentLibraryLifecycle::Published,
        AgentTemplateMutationKind::Retire => AgentLibraryLifecycle::Retired,
    };
    if committed.template.lifecycle != expected_lifecycle {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent template status kind does not match lifecycle"
                .to_string(),
        );
    }
    Ok(())
}

fn template_domain_result(result: &AgentTemplateCommittedResult) -> Result<MutationResult, String> {
    agent_row::domain_result(result, AGENT_TEMPLATE_RESULT_SCHEMA_ID, "agent template")
}

fn encode_template_domain_result(result: &AgentTemplateCommittedResult) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(
        &template_domain_result(result)?,
        "agent template domain result",
    )
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
fn replayed_template(
    replay: ReplayResolution,
    template_id: &str,
) -> Result<Option<(AgentTemplateWriteResult, MutationReceipt)>, String> {
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
            "CORRUPT_MUTATION_LEDGER: agent template replay is missing its typed receipt"
                .to_string(),
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
    Ok(Some((
        AgentTemplateWriteResult {
            result: committed,
            replayed: true,
        },
        receipt,
    )))
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the template surface.
pub type AgentTemplateStoreRef = Arc<AgentLibraryStore>;

#[cfg(test)]
mod tests;
