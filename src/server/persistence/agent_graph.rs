//! Durable revisions for agent GRAPHS (RF-ADR-008).
//!
//! A graph is published into the SAME owner file as the Agent Library entries
//! it composes — `agent_library.redb`, tables `agent_graph` /
//! `agent_graph_heads`. A graph is a composition of entries, not a separate
//! entity family, so a second physical owner would split one authority in two
//! (RF-RULING-004) and force any question about "what is this agent system"
//! to be answered by reconciling two files.
//!
//! # Why this mirrors [`super::agent_library`] rather than generalizing it
//!
//! The two record families share the whole revision protocol — head CAS,
//! append-only revisions, replay identity, typed receipt, outbox — and differ
//! only in their tables, their event names, their digest domains, and which
//! field carries the content digest. That is a trait's worth of difference,
//! and extracting one is the right end state.
//!
//! It is deliberately NOT the first step. Generalizing the library's durable
//! commit path means rewriting every existing agent-library write in order to
//! prove a graph feature that has never run: if the generic is subtly wrong,
//! the damage lands on a working, shipped path, and on the replay/receipt path
//! a subtle error is invisible rather than loud. A separate module cannot
//! break the library path at all.
//!
//! The safe order is to generalize from two *tested* implementations, not from
//! one plus a guess — so the shared machinery this module already reuses
//! (`resolve_nonce_first`, `admitted_context`, `batch_id`, `next_revision`,
//! `require_expected_revision`, `validate_context`, the scope handle and the
//! mutation kernel) stays shared, and the genuinely record-typed functions are
//! written out once more here. The resulting near-duplicate pairs are
//! registered in `dupehound-distinct.toml` with reasons rather than hidden.

use std::collections::BTreeMap;
use std::sync::Arc;

use redb::ReadableTable;

use eg_storage::{OwnedStoreHandle, RecordedOperation, ScopedRead};
use eg_transaction::{AdmittedOwnerWrite, Begin, ReplayResolution};

use eg_types::agent_graph::{
    AgentGraphCommittedResult, AgentGraphEntry, AgentGraphMutationKind, AgentGraphOutboxEvent,
    AgentGraphPublishRequest, AgentGraphRetireRequest, AgentGraphStatusRequest,
    AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
};
use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
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

mod write;

const AGENT_GRAPH_OUTBOX_TOPIC: &str = "eg.agent-graph.revision.v1";
const AGENT_GRAPH_RESULT_SCHEMA_ID: &str = "agent-graph-result.v1";
const MAX_AGENT_GRAPH_REVISIONS: usize = 16_384;
const MAX_AGENT_GRAPH_HISTORY_BYTES: usize = 256 * 1024 * 1024;

/// What a committed graph write returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentGraphWriteResult {
    pub result: AgentGraphCommittedResult,
    pub replayed: bool,
}

impl AgentLibraryStore {
    /// Resolve every cross-record reference one GRAPH publish makes, and
    /// return the composition facts admission stamps on the revision.
    ///
    /// L3 -> L1 REUSES the helper L2 -> L1 and L1 -> L1 already go through: the
    /// per-pin question is identical, and a second implementation of it is a
    /// second place for it to drift. A shape's components are its node data
    /// contracts, its decision predicates, its template bindings, its edge
    /// conditions and the synthesis evidence that justifies it, gathered by
    /// `AgentGraphDraft::pinned_components` so one slot cannot be resolved
    /// while a sibling slot is forgotten.
    ///
    /// L3 -> L3 is resolved last, by `validate_composition`, which also derives
    /// the composed work ceiling from the tree it walks.
    fn admit_graph_references_in_write(
        &self,
        write: &super::agent_pin_resolution::Write<'_>,
        tenant_id: &str,
        graph: &eg_types::agent_graph::AgentGraphDraft,
    ) -> Result<eg_types::agent_graph::CompositionFacts, String> {
        self.resolve_component_pins_in_write(
            write,
            tenant_id,
            "agent graph",
            &graph.pinned_components(),
        )?;
        super::agent_pin_resolution::resolve_agent_pins_in_write(
            write,
            tenant_id,
            "agent graph",
            &graph.shape.pinned_agents(),
        )?;
        let templates: Vec<super::agent_pin_resolution::TemplatePin<'_>> = graph
            .shape
            .pinned_templates()
            .into_iter()
            .map(|(template_id, definition_digest)| {
                super::agent_pin_resolution::TemplatePin {
                    template_id,
                    definition_digest,
                    // A graph node records no revision number, only the digest.
                    entry_revision: None,
                }
            })
            .collect();
        super::agent_pin_resolution::resolve_template_pins_in_write(
            write,
            tenant_id,
            "agent graph",
            &templates,
        )?;
        eg_types::agent_graph::validate_composition(tenant_id, &graph.shape, |graph_id, shape| {
            self.resolve_composed_graph(write, tenant_id, graph_id, shape)
        })
    }

    /// The head revision of one graph, or `None` if it was never published.
    pub fn current_graph(
        &self,
        tenant_id: &str,
        graph_id: &str,
    ) -> Result<Option<AgentGraphEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, graph_id)?;
        let read = self.read()?;
        Ok(read_graph_history(&read, tenant_id, graph_id)?
            .1
            .into_iter()
            .last())
    }

    /// Every retained revision of one graph, oldest first.
    pub fn graph_revisions(
        &self,
        tenant_id: &str,
        graph_id: &str,
    ) -> Result<Vec<AgentGraphEntry>, String> {
        eg_types::agent_library::validate_key(tenant_id, graph_id)?;
        let read = self.read()?;
        Ok(read_graph_history(&read, tenant_id, graph_id)?.1)
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn graph_status(
        &self,
        request: AgentGraphStatusRequest,
    ) -> Result<Option<AgentGraphWriteResult>, String> {
        validate_context(self, &request.context)?;
        eg_types::agent_library::validate_key(&request.context.tenant_id, &request.graph_id)?;
        let owner = self.scope_handle(&request.context.tenant_id)?;
        let batch_id = batch_id(&request.context.idempotency_key)?;
        let read = self.kernel.read_scope(&owner)?;
        let Some(record) = eg_transaction::read_ledger(&read, &batch_id)? else {
            return Ok(None);
        };
        let Some(result_bytes) = record.result_msgpack.as_ref() else {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent graph status has no typed result".to_string(),
            );
        };
        let committed = decode_graph_status_result(result_bytes)?;
        validate_graph_status_record(
            &record,
            owner.identity(),
            &request.context,
            &request.graph_id,
            request.kind,
            &committed,
        )?;
        Ok(Some(AgentGraphWriteResult {
            result: committed,
            replayed: true,
        }))
    }

    /// Resolve one pinned child revision for a composition check.
    ///
    /// Looked up under the PUBLISHER's tenant, so another tenant's graph is
    /// simply not found -- cross-tenant composition is impossible here by
    /// construction, and `validate_composition`'s own tenant check is the
    /// second line for any other resolver.
    ///
    /// `lifecycle` is the HEAD's, not the pinned revision's. A tombstone is a
    /// separate later revision, so the pinned one stays `Published` forever;
    /// asking the head is the only way to know whether the graph has since
    /// been withdrawn.
    fn resolve_composed_graph(
        &self,
        txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        graph_id: &str,
        shape_digest: &str,
    ) -> Result<eg_types::agent_graph::ResolvedGraph, String> {
        let head_revision = txn
            .open_read_table(eg_storage::AGENT_GRAPH_HEADS)?
            .get((tenant_id, graph_id))?
            .map(|value| value.value())
            .ok_or_else(|| "no such graph in this tenant".to_string())?;
        let revisions = txn.open_read_table(eg_storage::AGENT_GRAPH_REVISIONS)?;
        let head = revisions
            .get((tenant_id, graph_id, head_revision))?
            .ok_or_else(|| "agent graph head points to a missing revision".to_string())?;
        let head_lifecycle = decode_graph(head.value())?.lifecycle;

        // `range_from` is open-ended, so the prefix has to be re-checked per
        // row: without the break this walks into the NEXT graph's revisions
        // and could resolve a digest belonging to a different graph.
        let mut scanned = 0usize;
        for row in revisions.range_from((tenant_id, graph_id, 0))? {
            scanned += 1;
            if scanned > MAX_AGENT_GRAPH_REVISIONS {
                return Err("agent graph history exceeds its retained revision bound".to_string());
            }
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (row_tenant, row_graph, _) = key.value();
            if row_tenant != tenant_id || row_graph != graph_id {
                break;
            }
            let entry = decode_graph(value.value())?;
            if entry.shape_digest == shape_digest {
                return Ok(eg_types::agent_graph::ResolvedGraph {
                    shape: entry.shape,
                    lifecycle: head_lifecycle,
                    tenant_id: entry.tenant_id,
                });
            }
        }
        Err("no retained revision of that graph matches the pinned shape digest".to_string())
    }

    fn graph_at_revision_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        graph_id: &str,
        revision: u64,
    ) -> Result<Option<AgentGraphEntry>, String> {
        if revision == 0 {
            return Ok(None);
        }
        let revisions = write.open_read_table(eg_storage::AGENT_GRAPH_REVISIONS)?;
        let Some(value) = revisions.get((tenant_id, graph_id, revision))? else {
            return Ok(None);
        };
        let entry = decode_graph(value.value())?;
        if entry.tenant_id != tenant_id
            || entry.graph_id != graph_id
            || entry.entry_revision != revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent graph row does not match its physical key"
                    .to_string(),
            );
        }
        Ok(Some(entry))
    }
}

/// Head CAS plus the append-only revision row, in the graph tables.
fn apply_graph_rows(
    owner_write: &AdmittedOwnerWrite<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    expected_revision: u64,
    entry: &AgentGraphEntry,
    entry_bytes: &[u8],
) -> Result<(), String> {
    let mut heads = owner_write.open_table(eg_storage::AGENT_GRAPH_HEADS)?;
    let actual_revision = heads
        .get((context.tenant_id.as_str(), entry.graph_id.as_str()))
        .map_err(|error| error.to_string())?
        .map(|value| value.value())
        .unwrap_or(0);
    require_expected_revision(expected_revision, actual_revision)?;
    if entry.entry_revision != next_revision(expected_revision)? {
        return Err("agent graph entry revision does not follow its expected head".to_string());
    }
    let mut revisions = owner_write.open_table(eg_storage::AGENT_GRAPH_REVISIONS)?;
    if actual_revision > 0 {
        let current = revisions
            .get((
                context.tenant_id.as_str(),
                entry.graph_id.as_str(),
                actual_revision,
            ))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "agent graph head points to a missing revision".to_string())?;
        let current = decode_graph(current.value())?;
        if current.lifecycle == AgentLibraryLifecycle::Retired {
            return Err("retired agent graphs cannot be resurrected".to_string());
        }
    }
    if revisions
        .get((
            context.tenant_id.as_str(),
            entry.graph_id.as_str(),
            entry.entry_revision,
        ))
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("agent graph revision already exists".to_string());
    }
    revisions
        .insert(
            (
                context.tenant_id.as_str(),
                entry.graph_id.as_str(),
                entry.entry_revision,
            ),
            entry_bytes,
        )
        .map_err(|error| error.to_string())?;
    heads
        .insert(
            (context.tenant_id.as_str(), entry.graph_id.as_str()),
            entry.entry_revision,
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn graph_operations(
    kind: AgentGraphMutationKind,
    entry: &AgentGraphEntry,
) -> Vec<MutationOperation> {
    let event_type = match kind {
        AgentGraphMutationKind::Publish => "agent_graph_publish",
        AgentGraphMutationKind::Retire => "agent_graph_retire",
    };
    vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Lifecycle,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: event_type.to_string(),
            // The whole record, not just what it does: two revisions with the
            // same shape but different metadata are different mutations.
            query: entry.definition_digest.clone(),
        },
    }]
}

fn graph_outbox_headers(entry: &AgentGraphEntry) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_GRAPH_ENTRY_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), entry.tenant_id.clone()),
        ("graph_id".to_string(), entry.graph_id.clone()),
        (
            "entry_revision".to_string(),
            entry.entry_revision.to_string(),
        ),
        ("shape_digest".to_string(), entry.shape_digest.clone()),
        (
            "definition_digest".to_string(),
            entry.definition_digest.clone(),
        ),
        // The ceiling admission derived, on the receipt path. A consumer
        // reading the stream learns what this revision was admitted to cost
        // without re-resolving the composition tree.
        (
            "composed_work_ceiling".to_string(),
            entry.composed_work_ceiling.to_string(),
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

fn build_graph_batch(
    owner: &OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind: AgentGraphMutationKind,
    entry: &AgentGraphEntry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let key = format!(
        "{}:{}:{}",
        entry.tenant_id, entry.graph_id, entry.entry_revision
    );
    let operations = graph_operations(kind, entry);
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_GRAPH_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers: graph_outbox_headers(entry),
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

fn read_graph_history(
    read: &ScopedRead<'_, eg_storage::AgentLibraryOwner>,
    tenant_id: &str,
    graph_id: &str,
) -> Result<(Option<u64>, Vec<AgentGraphEntry>), String> {
    let head = read
        .open_owner_table(eg_storage::AGENT_GRAPH_HEADS)?
        .get((tenant_id, graph_id))
        .map_err(|error| error.to_string())?
        .map(|value| value.value());
    let table = read.open_owner_table(eg_storage::AGENT_GRAPH_REVISIONS)?;
    let mut entries = Vec::new();
    let mut bytes = 0usize;
    for row in table
        .range((tenant_id, graph_id, 0)..=(tenant_id, graph_id, u64::MAX))
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        // Bounded: a caller can otherwise ask for an unbounded amount of work
        // by publishing revisions.
        if entries.len() >= MAX_AGENT_GRAPH_REVISIONS {
            return Err("agent graph history exceeds its retained revision bound".to_string());
        }
        bytes = bytes.saturating_add(value.value().len());
        if bytes > MAX_AGENT_GRAPH_HISTORY_BYTES {
            return Err("agent graph history exceeds its retained byte bound".to_string());
        }
        let (row_tenant, row_graph, row_revision) = key.value();
        let entry = decode_graph(value.value())?;
        if entry.tenant_id != row_tenant
            || entry.graph_id != row_graph
            || entry.entry_revision != row_revision
        {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent graph row does not match its physical key"
                    .to_string(),
            );
        }
        entries.push(entry);
    }
    Ok((head, entries))
}

fn decode_graph(bytes: &[u8]) -> Result<AgentGraphEntry, String> {
    let entry: AgentGraphEntry = super::agent_row::decode(bytes, "agent graph row")?;
    entry.validate()?;
    Ok(entry)
}

fn decode_committed_result(bytes: &[u8]) -> Result<AgentGraphCommittedResult, String> {
    let result: AgentGraphCommittedResult = super::agent_row::decode(bytes, "agent graph result")?;
    result.graph.validate()?;
    Ok(result)
}

fn decode_graph_status_result(bytes: &[u8]) -> Result<AgentGraphCommittedResult, String> {
    let mutation_result: MutationResult =
        super::agent_row::decode(bytes, "agent graph status result")?;
    mutation_result.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = mutation_result
    else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph status result is not a DomainResult".to_string(),
        );
    };
    if schema_id.as_str() != AGENT_GRAPH_RESULT_SCHEMA_ID {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph status result has an unexpected schema"
                .to_string(),
        );
    }
    decode_committed_result(payload.as_slice())
}

fn validate_graph_status_record(
    record: &eg_types::MutationBatchRecord,
    owner_identity: &eg_types::MutationScopeIdentity,
    context: &AgentLibraryMutationContext,
    graph_id: &str,
    kind: AgentGraphMutationKind,
    committed: &AgentGraphCommittedResult,
) -> Result<(), String> {
    validate_graph_status_durable_state(record)?;
    validate_graph_status_identity(record, owner_identity, context, committed)?;
    validate_graph_status_result(record, graph_id, kind, committed)
}

fn validate_graph_status_durable_state(
    record: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    if record.status != MutationBatchStatus::Committed {
        return Err("CORRUPT_MUTATION_LEDGER: agent graph status is not committed".to_string());
    }
    record.validate()
}

fn validate_graph_status_identity(
    record: &eg_types::MutationBatchRecord,
    owner_identity: &eg_types::MutationScopeIdentity,
    context: &AgentLibraryMutationContext,
    committed: &AgentGraphCommittedResult,
) -> Result<(), String> {
    let expected_batch_id = batch_id(&context.idempotency_key)?;
    if record.identity != *owner_identity
        || record.batch.batch_id != expected_batch_id
        || committed.batch_id != expected_batch_id
        || record.batch.batch_id != committed.batch_id
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph status batch identity is invalid".to_string(),
        );
    }
    if record.committing_tenant()? != context.tenant_id
        || committed.graph.tenant_id != context.tenant_id
    {
        return Err("CORRUPT_MUTATION_LEDGER: agent graph status tenant is invalid".to_string());
    }
    Ok(())
}

fn validate_graph_status_result(
    record: &eg_types::MutationBatchRecord,
    graph_id: &str,
    kind: AgentGraphMutationKind,
    committed: &AgentGraphCommittedResult,
) -> Result<(), String> {
    if record.committed_version.target() != Some(committed.committed_version) {
        return Err("CORRUPT_MUTATION_LEDGER: agent graph status version is invalid".to_string());
    }
    if committed.graph.graph_id != graph_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph status resolved a different graph".to_string(),
        );
    }
    let expected_lifecycle = match kind {
        AgentGraphMutationKind::Publish => AgentLibraryLifecycle::Published,
        AgentGraphMutationKind::Retire => AgentLibraryLifecycle::Retired,
    };
    if committed.graph.lifecycle != expected_lifecycle {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph status kind does not match lifecycle".to_string(),
        );
    }
    Ok(())
}

fn graph_domain_result(result: &AgentGraphCommittedResult) -> Result<MutationResult, String> {
    super::agent_row::domain_result(result, AGENT_GRAPH_RESULT_SCHEMA_ID, "agent graph")
}

fn encode_graph_domain_result(result: &AgentGraphCommittedResult) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(&graph_domain_result(result)?, "agent graph domain result")
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
fn replayed_graph(
    replay: ReplayResolution,
    graph_id: &str,
) -> Result<Option<(AgentGraphWriteResult, MutationReceipt)>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different agent graph mutation"
                    .to_string(),
            );
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph replay is missing its typed receipt".to_string(),
        );
    };
    let receipt = *receipt;
    receipt.validate()?;
    let MutationResult::DomainResult { payload, .. } = &receipt.result else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph replay receipt carries no domain result"
                .to_string(),
        );
    };
    let committed = decode_committed_result(payload.as_slice())?;
    if committed.graph.graph_id != graph_id {
        return Err(
            "CORRUPT_MUTATION_LEDGER: agent graph replay resolved a different graph".to_string(),
        );
    }
    Ok(Some((
        AgentGraphWriteResult {
            result: committed,
            replayed: true,
        },
        receipt,
    )))
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the graph surface.
pub type AgentGraphStoreRef = Arc<AgentLibraryStore>;

#[cfg(test)]
mod references;
#[cfg(test)]
mod tests;
