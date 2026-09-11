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
    AgentGraphCommittedResult, AgentGraphEntry, AgentGraphMutationKind,
    AgentGraphOutboxEvent, AgentGraphPublishRequest, AgentGraphRetireRequest,
    AgentGraphStatusRequest, AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
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

const AGENT_GRAPH_OUTBOX_TOPIC: &str = "eg.agent-graph.revision.v1";
const AGENT_GRAPH_RESULT_SCHEMA_ID: &str = "agent-graph-result.v1";
const MAX_AGENT_GRAPH_ROW_BYTES: usize = 16 * 1024 * 1024;
const MAX_AGENT_GRAPH_ROW_ITEMS: usize = 200_000;
const MAX_AGENT_GRAPH_REVISIONS: usize = 16_384;
const MAX_AGENT_GRAPH_HISTORY_BYTES: usize = 256 * 1024 * 1024;

/// What a committed graph write returns to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentGraphWriteResult {
    pub result: AgentGraphCommittedResult,
    pub replayed: bool,
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
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent graph writes require an explicit expected_revision".to_string())?;
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
            match admitted_context(&request.context, "agent-graph:publish") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        // The replay identity is minted from the DRAFT digest, not the shape
        // digest. Two publishes of the same shape that differ in version or
        // synthesis evidence are different operations: keying on the shape
        // alone made a retry that CORRECTED the evidence resolve as a replay of
        // the uncorrected publish, dropping the correction and reporting
        // success. The ceiling is deliberately outside this digest -- it is not
        // derived until the composition is resolved below, and a retry must be
        // able to replay a committed publish without re-resolving children that
        // may have been retired since.
        let draft_digest = eg_types::agent_graph::draft_definition_digest(&request.graph);
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "graph-publish",
            &request.graph.graph_id,
            expected_revision,
            Some(&draft_digest),
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
        if let Some(result) = match replayed_graph(replay, &request.graph.graph_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        // Every reference this shape makes is resolved INSIDE the write
        // transaction, so the records it resolves are the ones this commit will
        // actually be ordered against: its components (L3 -> L1), the agents
        // its `Agent` nodes run (L3 -> L2), the templates its `Template` nodes
        // instantiate, and the child graphs it composes (L3 -> L3, which also
        // derives the composed work ceiling). See
        // `admit_graph_references_in_write`.
        let composition = match self.admit_graph_references_in_write(
            &txn,
            &request.context.tenant_id,
            &request.graph,
        ) {
            Ok(facts) => facts,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        // The composed ceiling is STAMPED ON THE ENTRY, inside its definition
        // digest, and echoed in the outbox headers. It is what an executor and
        // `kg-delegate` have to honour, and neither can recompute it without
        // re-resolving the whole tree -- so discarding it here (as this code
        // once did) left delegation with nothing to check a caller's declared
        // ceiling against, and it was accepted unverified.
        let entry = match AgentGraphEntry::create(
            request.graph,
            composition.total_work,
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
        self.resolve_agent_pins_in_write(
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
        self.resolve_template_pins_in_write(write, tenant_id, "agent graph", &templates)?;
        eg_types::agent_graph::validate_composition(tenant_id, &graph.shape, |graph_id, shape| {
            self.resolve_composed_graph(write, tenant_id, graph_id, shape)
        })
    }

    /// Retain the current graph revision as a durable tombstone.
    pub fn retire_graph(
        &self,
        request: AgentGraphRetireRequest,
    ) -> Result<AgentGraphWriteResult, String> {
        validate_context(self, &request.context)?;
        let expected_revision = request
            .context
            .expected_revision
            .ok_or_else(|| "agent graph writes require an explicit expected_revision".to_string())?;
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
            match admitted_context(&request.context, "agent-graph:retire") {
                Ok(context) => context,
                Err(error) => {
                    txn.abort()?;
                    return Err(error);
                }
            };
        let operation = match agent_library_operation_identity(
            &owner,
            &replay_context,
            "graph-retire",
            &request.graph_id,
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
        if let Some(result) = match replayed_graph(replay, &request.graph_id) {
            Ok(replayed) => replayed,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        } {
            self.mutations.commit_replay_receipt(txn)?;
            return Ok(result);
        }

        let current = match self.graph_at_revision_in_write(
            &txn,
            &request.context.tenant_id,
            &request.graph_id,
            expected_revision,
        ) {
            Ok(Some(current)) => current,
            Ok(None) => {
                txn.abort()?;
                return Err("agent graph has no revision to retire".to_string());
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
        let committed = decode_committed_result(result_bytes)?;
        if committed.graph.graph_id != request.graph_id {
            return Err(
                "CORRUPT_MUTATION_LEDGER: agent graph status resolved a different graph"
                    .to_string(),
            );
        }
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
        let operations = graph_operations(kind, &entry);
        let policy_digest = effective_agent_library_policy_digest(&operations)?;
        let mut admitted = context.clone();
        admitted.policy_digest = format!("sha256:{}", policy_digest.to_hex());

        let event = AgentGraphOutboxEvent {
            schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
            kind,
            graph: entry.clone(),
            performing_actor: admitted.caller_principal.clone(),
            action_actor_scope: admitted.actor_scope.clone(),
        };
        event.validate()?;
        let event_bytes = eg_storage::encode_bounded(&event, "agent graph outbox event")?;
        let entry_bytes = eg_storage::encode_bounded(&entry, "agent graph revision")?;
        let batch_id = batch_id(&admitted.idempotency_key)?;

        let authoritative_version = match self.mutations.current_version(&txn, owner) {
            Ok(version) => version,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };
        let batch = match build_graph_batch(
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
                return Err("agent graph admission has no native source version".to_string());
            }
            Begin::Replay(_) => {
                txn.abort()?;
                return Err("CORRUPT_MUTATION_LEDGER: replay became visible after a fresh admission decision".to_string());
            }
        };
        if source_version != authoritative_version {
            txn.abort()?;
            return Err("agent graph source version changed while admitting write".to_string());
        }
        let committed_version = match source_version.checked_add(1) {
            Some(version) => version,
            None => {
                txn.abort()?;
                return Err("agent graph committed version overflow".to_string());
            }
        };
        let stable_result = AgentGraphCommittedResult {
            schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
            graph: entry.clone(),
            batch_id: batch_id.clone(),
            committed_version,
        };
        let result_bytes = match encode_graph_domain_result(&stable_result) {
            Ok(bytes) => bytes,
            Err(error) => {
                txn.abort()?;
                return Err(error);
            }
        };

        let owner_write_result: Result<(), String> = (|| {
            let owner_write = txn.owner_rows(owner, &batch)?;
            apply_graph_rows(
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
            entry.tenant_id, entry.graph_id, entry.entry_revision
        );
        let headers = graph_outbox_headers(&entry);
        let receipt = match owner_receipt(
            operation,
            nonce,
            &batch,
            "agent-graph",
            AGENT_GRAPH_OUTBOX_TOPIC,
            &key,
            &event_bytes,
            &headers,
            graph_domain_result(&stable_result)?,
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
            .ok_or_else(|| "agent graph commit has no target version".to_string())?;
        if recorded_version != committed_version {
            txn.abort()?;
            return Err("agent graph result version differs from the committed version".to_string());
        }
        self.mutations.commit(txn, &batch)?;
        Ok(AgentGraphWriteResult {
            result: stable_result,
            replayed: false,
        })
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
        ("definition_actor_scope".to_string(), entry.actor_scope.clone()),
        ("definition_purpose_id".to_string(), entry.purpose_id.clone()),
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
    let entry = eg_types::msgpack::decode_bounded::<AgentGraphEntry>(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AGENT_GRAPH_ROW_BYTES,
            MAX_AGENT_GRAPH_ROW_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "agent graph row is invalid or exceeds resource limits".to_string())?;
    entry.validate()?;
    Ok(entry)
}

fn decode_committed_result(bytes: &[u8]) -> Result<AgentGraphCommittedResult, String> {
    let result = eg_types::msgpack::decode_bounded::<AgentGraphCommittedResult>(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_AGENT_GRAPH_ROW_BYTES,
            MAX_AGENT_GRAPH_ROW_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "agent graph result is invalid or exceeds resource limits".to_string())?;
    result.graph.validate()?;
    Ok(result)
}

fn graph_domain_result(result: &AgentGraphCommittedResult) -> Result<MutationResult, String> {
    let payload = eg_storage::encode_bounded(result, "agent graph domain result payload")?;
    let payload = eg_types::contract::RecordBytes::new(payload)?;
    Ok(MutationResult::DomainResult {
        schema_id: eg_types::contract::SchemaId::new(AGENT_GRAPH_RESULT_SCHEMA_ID)?,
        payload_digest: payload.digest()?,
        payload,
    })
}

fn encode_graph_domain_result(result: &AgentGraphCommittedResult) -> Result<Vec<u8>, String> {
    eg_storage::encode_bounded(&graph_domain_result(result)?, "agent graph domain result")
}

/// Turn a resolved replay into a caller result, or `None` when it is fresh.
fn replayed_graph(
    replay: ReplayResolution,
    graph_id: &str,
) -> Result<Option<AgentGraphWriteResult>, String> {
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
    Ok(Some(AgentGraphWriteResult {
        result: committed,
        replayed: true,
    }))
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the graph surface.
pub type AgentGraphStoreRef = Arc<AgentLibraryStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_graph::{
        AgentGraphDraft, AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind, AgentGraphShape,
    };
    use eg_types::agent_component::ComponentDependency;
    use eg_types::contract::Nonce;

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    /// The seeding nonce index each pinned record owns.
    ///
    /// Fixed here rather than taken from a counter for two reasons: two
    /// fixtures that seed the same record must agree, and no two records may
    /// share a seeding nonce -- a reused attempt nonce is
    /// `REPLAY_NONCE_CONSUMED`, not a silent no-op. The agent indices are
    /// spaced by ten because seeding an agent also seeds the five components it
    /// is assembled from, starting at its own index.
    fn seed_index(record_id: &str) -> u8 {
        match record_id {
            "contract:findings" => 0,
            "contract:report" => 1,
            "contract:mismatch" => 2,
            "evidence:run-17" => 3,
            "agent:research" => 10,
            "agent:write" => 20,
            "agent:work" => 30,
            other => panic!("no seeding nonce reserved for '{other}'"),
        }
    }

    /// Seed a schema component and return the pin that resolves it.
    ///
    /// A graph publish now RESOLVES every component its shape pins, so a
    /// fixture can no longer invent a digest: the record has to exist, in this
    /// tenant, at exactly this revision.
    fn component(
        store: &AgentLibraryStore,
        tenant_id: &str,
        reference: &str,
    ) -> ComponentDependency {
        super::super::agent_component::seed_component_for_test(
            store,
            tenant_id,
            reference,
            eg_types::agent_component::AgentComponentKind::Schema,
            seed_index(reference),
        )
    }

    /// Seed an agent and return the node kind that runs it, pinned to its real
    /// `definition_digest`.
    fn agent_node(store: &AgentLibraryStore, tenant_id: &str, agent_id: &str) -> AgentGraphNodeKind {
        AgentGraphNodeKind::Agent {
            agent_id: agent_id.to_string(),
            definition_digest: super::super::agent_library::seed_agent_for_test(
                store,
                tenant_id,
                agent_id,
                seed_index(agent_id),
            ),
        }
    }

    /// research -> write -> end, contracts agreeing across each edge.
    fn shape(store: &AgentLibraryStore, tenant_id: &str) -> AgentGraphShape {
        let findings = component(store, tenant_id, "contract:findings");
        AgentGraphShape {
            entry_node: "research".into(),
            nodes: vec![
                AgentGraphNode {
                    node_id: "research".into(),
                    kind: agent_node(store, tenant_id, "agent:research"),
                    deps_contract: None,
                    output_contract: Some(findings.clone()),
                },
                AgentGraphNode {
                    node_id: "write".into(),
                    kind: agent_node(store, tenant_id, "agent:write"),
                    deps_contract: Some(findings),
                    output_contract: Some(component(store, tenant_id, "contract:report")),
                },
                AgentGraphNode {
                    node_id: "done".into(),
                    kind: AgentGraphNodeKind::End,
                    deps_contract: None,
                    output_contract: None,
                },
            ],
            edges: vec![
                AgentGraphEdge {
                    from: "research".into(),
                    to: "write".into(),
                    condition: None,
                },
                AgentGraphEdge {
                    from: "write".into(),
                    to: "done".into(),
                    condition: None,
                },
            ],
            max_iterations: 10,
        }
    }

    fn draft(store: &AgentLibraryStore, tenant_id: &str, graph_id: &str) -> AgentGraphDraft {
        AgentGraphDraft {
            graph_id: graph_id.to_string(),
            version: "1.0.0".to_string(),
            shape: shape(store, tenant_id),
            tenant_id: tenant_id.to_string(),
            actor_scope: "action-scope:a".to_string(),
            purpose_id: "agent-graph:publish".to_string(),
            policy_digest: super::super::agent_library::current_agent_library_policy_digest()
                .unwrap(),
            synthesis_evidence: None,
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
            policy_decision_id: "agent-graph:decision:policy-v1".to_string(),
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
    fn a_published_graph_is_durable_and_reads_back() {
        let (_dir, store) = open_store();
        let published = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        assert!(!published.replayed);
        assert_eq!(published.result.graph.entry_revision, 1);
        assert_eq!(
            published.result.graph.shape_digest,
            shape(&store, "tenant-a").shape_digest()
        );

        let current = store.current_graph("tenant-a", "graph-a").unwrap().unwrap();
        assert_eq!(current, published.result.graph);
        assert_eq!(store.graph_revisions("tenant-a", "graph-a").unwrap().len(), 1);
    }

    #[test]
    fn a_byte_identical_retry_replays_rather_than_publishing_twice() {
        // The property the whole replay/nonce/receipt path exists for: a
        // transport retry must return the SAME committed result, not a second
        // revision.
        let (_dir, store) = open_store();
        let first = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();

        let mut retry_context = context(&store, "tenant-a", "key-1", 2, 0, "agent-graph:publish");
        retry_context.created_at_ms = 99;
        let replayed = store
            .publish_graph(AgentGraphPublishRequest {
                context: retry_context,
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        assert!(replayed.replayed, "a retry must replay, not re-publish");
        assert_eq!(replayed.result, first.result);
        assert_eq!(
            store.graph_revisions("tenant-a", "graph-a").unwrap().len(),
            1,
            "a replay must not append a second revision"
        );
    }

    #[test]
    fn a_retry_that_corrects_the_synthesis_evidence_is_not_a_replay() {
        // The publish replay identity used to be minted from `shape_digest`
        // alone, so a retry of a timed-out publish that CORRECTED the synthesis
        // evidence resolved as a replay of the uncorrected commit: the
        // correction was silently dropped and the caller was told it worked.
        // Keyed on the draft digest it is a different operation, and reusing
        // the key for it is a NAMED refusal instead.
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();

        let mut corrected = draft(&store, "tenant-a", "graph-a");
        corrected.synthesis_evidence = Some(component(&store, "tenant-a", "evidence:run-17"));
        assert_eq!(
            corrected.shape.shape_digest(),
            draft(&store, "tenant-a", "graph-a").shape.shape_digest(),
            "the shape is untouched, which is what made this a false replay"
        );
        let mut retry = context(&store, "tenant-a", "key-1", 2, 0, "agent-graph:publish");
        retry.created_at_ms = 99;
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: retry,
                graph: corrected,
            })
            .unwrap_err();
        assert!(error.contains("IDEMPOTENCY_CONFLICT"), "got: {error}");
        assert_eq!(
            store.graph_revisions("tenant-a", "graph-a").unwrap()[0].synthesis_evidence,
            None,
            "and the committed revision is untouched"
        );
    }

    #[test]
    fn the_admitted_ceiling_is_persisted_on_the_revision() {
        // `kg-delegate` checks a caller's declared `composed_work_ceiling`
        // against this field. It was computed inside the write transaction and
        // then discarded (`let _ = composition;`), under a comment claiming the
        // outbox headers recorded it -- they did not, and neither did anything
        // else, so delegation had nothing to compare against.
        let (_dir, store) = open_store();
        let published = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        // No child graphs, so the composed ceiling is the shape's own bound.
        assert_eq!(
            published.result.graph.composed_work_ceiling,
            u64::from(shape(&store, "tenant-a").max_iterations)
        );
        let current = store.current_graph("tenant-a", "graph-a").unwrap().unwrap();
        assert_eq!(
            current.composed_work_ceiling,
            published.result.graph.composed_work_ceiling,
            "it must survive the round trip through redb"
        );
        current.validate().expect("the persisted row re-derives its own digest");
    }

    #[test]
    fn reusing_an_attempt_nonce_is_refused() {
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 7, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        // Same nonce, DIFFERENT idempotency key: the nonce is single-use, so
        // this is a replayed attempt aimed at a different operation.
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-2", 7, 1, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-b"),
            })
            .unwrap_err();
        assert!(error.contains("REPLAY_NONCE_CONSUMED"), "got: {error}");
    }

    #[test]
    fn a_stale_expected_revision_is_refused() {
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        // Head is 1; publishing against 0 again is a lost update.
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-2", 2, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap_err();
        assert!(!error.is_empty());
        assert_eq!(
            store.graph_revisions("tenant-a", "graph-a").unwrap().len(),
            1,
            "a refused write must leave no revision behind"
        );
    }

    #[test]
    fn a_retired_graph_is_a_tombstone_that_cannot_be_resurrected() {
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        let retired = store
            .retire_graph(AgentGraphRetireRequest {
                context: context(&store, "tenant-a", "key-2", 2, 1, "agent-graph:retire"),
                graph_id: "graph-a".to_string(),
            })
            .unwrap();
        assert_eq!(retired.result.graph.lifecycle, AgentLibraryLifecycle::Retired);
        assert_eq!(retired.result.graph.entry_revision, 2);

        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-3", 3, 2, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap_err();
        assert!(error.contains("resurrected"), "got: {error}");
    }

    #[test]
    fn graphs_are_scoped_to_their_tenant() {
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .unwrap();
        assert!(store.current_graph("tenant-b", "graph-a").unwrap().is_none());
    }

    #[test]
    fn a_graph_and_an_entry_of_the_same_id_do_not_collide() {
        // Both record families live in ONE owner file. If they shared a key
        // space, publishing a graph would overwrite the agent it composes.
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "same-id"),
            })
            .unwrap();
        assert!(store.current("tenant-a", "same-id").unwrap().is_none());
        assert!(store.current_graph("tenant-a", "same-id").unwrap().is_some());
    }

    #[test]
    fn an_unsound_shape_never_reaches_the_ledger() {
        let (_dir, store) = open_store();
        let mut broken = draft(&store, "tenant-a", "graph-a");
        // The consumer requires an input its producer does not produce.
        broken.shape.nodes[1].deps_contract =
            Some(component(&store, "tenant-a", "contract:mismatch"));
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: broken,
            })
            .unwrap_err();
        assert!(error.contains("contracts agree"), "got: {error}");
        assert!(store.current_graph("tenant-a", "graph-a").unwrap().is_none());
    }

    // ---- reference resolution: the pins a shape makes to L1, L2 and
    // ---- templates ----
    //
    // Every one of these publishes today by construction -- the fixtures seed
    // what they pin. What none of them could do before is FAIL: a shape's
    // `Agent`, `Template` and component pins were checked for well-formed text
    // and a well-formed `sha256:<hex>` and nothing else, so 64 invented hex
    // characters published a graph claiming an agent it was never composed
    // against, and `shape_digest` then attested to the claim.

    #[test]
    fn a_graph_pinning_an_agent_that_does_not_exist_is_refused() {
        let (_dir, store) = open_store();
        let mut graph = draft(&store, "tenant-a", "graph-a");
        let AgentGraphNodeKind::Agent {
            definition_digest, ..
        } = graph.shape.nodes[0].kind.clone()
        else {
            panic!("the fixture's entry node runs an agent");
        };
        // A real digest, a name nothing carries: resolution is by (id, digest),
        // so BOTH halves have to resolve or a leaked digest is a grant.
        graph.shape.nodes[0].kind = AgentGraphNodeKind::Agent {
            agent_id: "agent:ghost".into(),
            definition_digest,
        };
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("an unresolvable agent pin must be refused");
        assert!(error.contains("which does not exist in this tenant"), "got: {error}");
        assert!(store.current_graph("tenant-a", "graph-a").unwrap().is_none());
    }

    #[test]
    fn a_graph_pinning_an_invented_agent_digest_is_refused() {
        let (_dir, store) = open_store();
        let mut graph = draft(&store, "tenant-a", "graph-a");
        graph.shape.nodes[0].kind = AgentGraphNodeKind::Agent {
            agent_id: "agent:research".into(),
            definition_digest: digest('7'),
        };
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("a digest no revision carries must be refused");
        assert!(error.contains("that was never published"), "got: {error}");
    }

    #[test]
    fn a_graph_pinning_another_tenants_agent_is_refused() {
        // The pin resolves under the PUBLISHER's tenant, so another tenant's
        // agent is simply not found -- a caller who learns a digest must not be
        // able to run an agent it was never granted.
        let (_dir, store) = open_store();
        let foreign = super::super::agent_library::seed_agent_for_test(
            &store,
            "tenant-b",
            "agent:foreign",
            40,
        );
        let mut graph = draft(&store, "tenant-a", "graph-a");
        graph.shape.nodes[0].kind = AgentGraphNodeKind::Agent {
            agent_id: "agent:foreign".into(),
            definition_digest: foreign,
        };
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("a cross-tenant agent pin must be refused");
        assert!(error.contains("which does not exist in this tenant"), "got: {error}");
    }

    #[test]
    fn a_graph_pinning_a_retired_agent_is_refused_while_old_pins_still_resolve() {
        // The retained-but-not-buildable rule composition already applies to
        // child graphs, applied to the agents a shape runs.
        let (_dir, store) = open_store();
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-a"),
            })
            .expect("the first graph publishes while its agents are live");
        store
            .retire(eg_types::agent_library::AgentLibraryRetireRequest {
                context: context(&store, "tenant-a", "key-retire", 2, 1, "agent-library:retire"),
                agent_id: "agent:research".to_string(),
            })
            .expect("the agent retires");
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-2", 3, 0, "agent-graph:publish"),
                graph: draft(&store, "tenant-a", "graph-b"),
            })
            .expect_err("nothing new may be built on a withdrawn agent");
        assert!(error.contains("which is retired"), "got: {error}");
        // The graph published before the retirement is untouched, and the agent
        // stays readable, because that graph still has to resolve it.
        assert!(store.current_graph("tenant-a", "graph-a").unwrap().is_some());
        assert!(store.current("tenant-a", "agent:research").unwrap().is_some());
    }

    #[test]
    fn a_graph_pinning_synthesis_evidence_that_does_not_exist_is_refused() {
        // The L3 -> L1 edge. RF-ADR-008 says the evidence is what makes a
        // synthesized shape auditable; it only is if the record it names exists.
        let (_dir, store) = open_store();
        let mut graph = draft(&store, "tenant-a", "graph-a");
        graph.synthesis_evidence = Some(ComponentDependency {
            component_id: "evidence:never-published".into(),
            kind: eg_types::agent_component::AgentComponentKind::Schema,
            definition_digest: digest('7'),
        });
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("an unresolvable component pin must be refused");
        assert!(error.contains("which does not exist in this tenant"), "got: {error}");
    }

    #[test]
    fn a_graph_pinning_a_component_under_the_wrong_kind_is_refused() {
        // A shape's data contracts must be SCHEMA components. Structural
        // validation checks the kind the pin DECLARES; only resolution can
        // check the kind the named record actually has.
        let (_dir, store) = open_store();
        let model = super::super::agent_component::seed_component_for_test(
            &store,
            "tenant-a",
            "model-profile:mislabelled",
            eg_types::agent_component::AgentComponentKind::ModelProfile,
            50,
        );
        let mut graph = draft(&store, "tenant-a", "graph-a");
        graph.synthesis_evidence = Some(ComponentDependency {
            component_id: model.component_id,
            kind: eg_types::agent_component::AgentComponentKind::Schema,
            definition_digest: model.definition_digest,
        });
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("a pin whose kind does not match the record must be refused");
        assert!(error.contains("but it is a model_profile"), "got: {error}");
    }

    #[test]
    fn a_graph_instantiating_a_template_resolves_it_and_refuses_a_ghost() {
        let (_dir, store) = open_store();
        let template_digest = super::super::agent_pin_resolution::seed_template_for_test(
            &store,
            "tenant-a",
            "template:researcher",
            60,
        );
        let mut graph = draft(&store, "tenant-a", "graph-a");
        graph.shape.nodes[0].kind = AgentGraphNodeKind::Template {
            template_id: "template:researcher".into(),
            definition_digest: template_digest.clone(),
            bindings: BTreeMap::new(),
        };
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                graph: graph.clone(),
            })
            .expect("a template node pinning a real template publishes");

        // And the same shape naming a template nothing carries does not.
        let mut ghost = draft(&store, "tenant-a", "graph-b");
        ghost.shape.nodes[0].kind = AgentGraphNodeKind::Template {
            template_id: "template:ghost".into(),
            definition_digest: template_digest,
            bindings: BTreeMap::new(),
        };
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-2", 2, 0, "agent-graph:publish"),
                graph: ghost,
            })
            .expect_err("an unresolvable template pin must be refused");
        assert!(error.contains("which does not exist in this tenant"), "got: {error}");
    }

    // ---- composition: a graph whose node is another GRAPH ----
    //
    // Nothing below this line was covered before. No store test published a
    // shape containing an `AgentGraphNodeKind::Graph` node, so
    // `resolve_composed_graph` -- the one reference edge this hierarchy
    // actually resolved -- had no coverage at all, and neither did the composed
    // ceiling that admission stamps from it.

    /// The child: one agent, then end, producing `contract:report`.
    fn child_shape(
        store: &AgentLibraryStore,
        tenant_id: &str,
        max_iterations: u32,
    ) -> AgentGraphShape {
        AgentGraphShape {
            entry_node: "work".into(),
            nodes: vec![
                AgentGraphNode {
                    node_id: "work".into(),
                    kind: agent_node(store, tenant_id, "agent:work"),
                    deps_contract: None,
                    output_contract: Some(component(store, tenant_id, "contract:report")),
                },
                AgentGraphNode {
                    node_id: "done".into(),
                    kind: AgentGraphNodeKind::End,
                    deps_contract: None,
                    output_contract: None,
                },
            ],
            edges: vec![AgentGraphEdge {
                from: "work".into(),
                to: "done".into(),
                condition: None,
            }],
            max_iterations,
        }
    }

    /// A parent whose single step runs the named child graph, pinned by shape.
    fn parent_shape(
        store: &AgentLibraryStore,
        tenant_id: &str,
        child_id: &str,
        child_shape_digest: &str,
        max_iterations: u32,
    ) -> AgentGraphShape {
        AgentGraphShape {
            entry_node: "team".into(),
            nodes: vec![
                AgentGraphNode {
                    node_id: "team".into(),
                    kind: AgentGraphNodeKind::Graph {
                        graph_id: child_id.into(),
                        shape_digest: child_shape_digest.into(),
                    },
                    deps_contract: None,
                    output_contract: Some(component(store, tenant_id, "contract:report")),
                },
                AgentGraphNode {
                    node_id: "done".into(),
                    kind: AgentGraphNodeKind::End,
                    deps_contract: None,
                    output_contract: None,
                },
            ],
            edges: vec![AgentGraphEdge {
                from: "team".into(),
                to: "done".into(),
                condition: None,
            }],
            max_iterations,
        }
    }

    fn publish_shape(
        store: &AgentLibraryStore,
        graph_id: &str,
        shape: AgentGraphShape,
        key: &str,
        nonce: u8,
    ) -> AgentGraphEntry {
        let mut graph = draft(store, "tenant-a", graph_id);
        graph.shape = shape;
        store
            .publish_graph(AgentGraphPublishRequest {
                context: context(store, "tenant-a", key, nonce, 0, "agent-graph:publish"),
                graph,
            })
            .expect("publishes")
            .result
            .graph
    }

    #[test]
    fn a_nested_graph_publishes_and_its_composed_ceiling_is_the_product() {
        let (_dir, store) = open_store();
        let child = publish_shape(&store, "graph:child", child_shape(&store, "tenant-a", 4), "key-child", 1);
        assert_eq!(child.composed_work_ceiling, 4);

        let parent = publish_shape(
            &store,
            "graph:parent",
            parent_shape(&store, "tenant-a", "graph:child", &child.shape_digest, 3),
            "key-parent",
            2,
        );
        // The product, not either level: 3 x 4. This is the number
        // `kg-delegate` holds a caller's declared ceiling to.
        assert_eq!(parent.composed_work_ceiling, 12);
        let reread = store
            .current_graph("tenant-a", "graph:parent")
            .unwrap()
            .unwrap();
        assert_eq!(reread, parent);
        reread
            .validate()
            .expect("the persisted row re-derives its own digest");
    }

    #[test]
    fn a_nested_graph_pinning_a_shape_no_revision_carries_is_refused() {
        // The whole point of resolving a composition inside the write
        // transaction: a pinned child is a claim, and an unresolvable claim must
        // not become a durable revision.
        let (_dir, store) = open_store();
        publish_shape(&store, "graph:child", child_shape(&store, "tenant-a", 4), "key-child", 1);
        let mut graph = draft(&store, "tenant-a", "graph:parent");
        graph.shape = parent_shape(&store, "tenant-a", "graph:child", &digest('7'), 3);
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-parent", 2, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("an unresolvable pin must be refused");
        assert!(
            error.contains("no retained revision of that graph matches the pinned shape digest"),
            "got: {error}"
        );
        assert!(store
            .current_graph("tenant-a", "graph:parent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn composing_a_child_from_another_tenant_is_refused() {
        // Resolution is by (id, digest) alone, so a caller who learns a digest
        // must not be able to execute a graph it was never granted.
        let (_dir, store) = open_store();
        let mut child = draft(&store, "tenant-b", "graph:child");
        child.shape = child_shape(&store, "tenant-b", 4);
        let child = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-b", "key-child", 1, 0, "agent-graph:publish"),
                graph: child,
            })
            .unwrap()
            .result
            .graph;

        let mut graph = draft(&store, "tenant-a", "graph:parent");
        graph.shape = parent_shape(&store, "tenant-a", "graph:child", &child.shape_digest, 3);
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-parent", 2, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("a cross-tenant composition must be refused");
        assert!(error.contains("no such graph in this tenant"), "got: {error}");
    }

    #[test]
    fn composing_a_retired_child_is_refused_while_it_stays_resolvable() {
        let (_dir, store) = open_store();
        let child = publish_shape(&store, "graph:child", child_shape(&store, "tenant-a", 4), "key-child", 1);
        store
            .retire_graph(eg_types::agent_graph::AgentGraphRetireRequest {
                context: context(&store, "tenant-a", "key-retire", 2, 1, "agent-graph:retire"),
                graph_id: "graph:child".to_string(),
            })
            .unwrap();
        let mut graph = draft(&store, "tenant-a", "graph:parent");
        graph.shape = parent_shape(&store, "tenant-a", "graph:child", &child.shape_digest, 3);
        let error = store
            .publish_graph(AgentGraphPublishRequest {
                context: context(&store, "tenant-a", "key-parent", 3, 0, "agent-graph:publish"),
                graph,
            })
            .expect_err("nothing new may be built on a withdrawn graph");
        assert!(error.contains("which is retired"), "got: {error}");
        // The retired revision is still RESOLVABLE by id -- a parent published
        // before the retirement keeps working.
        assert!(store
            .current_graph("tenant-a", "graph:child")
            .unwrap()
            .is_some());
    }

    #[test]
    fn a_three_level_composition_stamps_the_product_across_every_level() {
        // Two levels top out at 1000 x 1000 with two legal shapes, so the
        // danger the ceiling exists for -- the PRODUCT across levels, each of
        // which looks modest on its own -- is only reachable at depth >= 2.
        let (_dir, store) = open_store();
        let leaf = publish_shape(&store, "graph:leaf", child_shape(&store, "tenant-a", 5), "key-leaf", 1);
        let mid = publish_shape(
            &store,
            "graph:mid",
            parent_shape(&store, "tenant-a", "graph:leaf", &leaf.shape_digest, 7),
            "key-mid",
            2,
        );
        assert_eq!(mid.composed_work_ceiling, 35);
        let root = publish_shape(
            &store,
            "graph:root",
            parent_shape(&store, "tenant-a", "graph:mid", &mid.shape_digest, 11),
            "key-root",
            3,
        );
        assert_eq!(
            root.composed_work_ceiling, 385,
            "11 x 7 x 5 -- not 11, not 77, and not the deepest level alone"
        );
    }

    #[test]
    fn the_store_reopens_after_a_graph_commit() {
        // Recovery validates every owner row against its parent batch on open.
        // A graph write that recovery cannot re-derive would make the whole
        // owner -- entries included -- unopenable.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        {
            let store = AgentLibraryStore::open(path).unwrap();
            store
                .publish_graph(AgentGraphPublishRequest {
                    context: context(&store, "tenant-a", "key-1", 1, 0, "agent-graph:publish"),
                    graph: draft(&store, "tenant-a", "graph-a"),
                })
                .unwrap();
        }
        let reopened = AgentLibraryStore::open(path).expect("owner reopens after a graph commit");
        let current = reopened
            .current_graph("tenant-a", "graph-a")
            .unwrap()
            .expect("the graph survives a reopen");
        assert_eq!(current.entry_revision, 1);
        assert_eq!(current.shape_digest, shape(&reopened, "tenant-a").shape_digest());
    }
}
