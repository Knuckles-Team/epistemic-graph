//! Durable revisions for agent GRAPHS (RF-ADR-008).
//!
//! A graph is published into the SAME owner file as the Agent Library entries
//! it composes — `agent_library.redb`, tables `agent_graph` /
//! `agent_graph_heads`. A graph is a composition of entries, not a separate
//! entity family, so a second physical owner would split one authority in two
//! (RF-RULING-004) and force any question about "what is this agent system"
//! to be answered by reconciling two files.
//!
//! # The shared revision protocol
//!
//! Graphs share the whole revision protocol with components and templates --
//! head CAS, append-only revisions, replay identity, typed receipt, outbox --
//! and differ only in their tables, their event names, their digest domains,
//! and which field carries the record id. That difference is
//! [`super::agent_revision::RevisionLayer`], implemented here by
//! [`GraphLayer`]; the protocol itself is written once, in
//! [`super::agent_revision`].
//!
//! It was generalized only once three tested implementations existed, so the
//! generic is proven against the behaviour each copy's own tests pin rather
//! than against a guess. What is genuinely graph-shaped stays here: reference
//! admission and composition, the composed work ceiling, and the extra outbox
//! headers that carry them.

use std::collections::BTreeMap;
use std::sync::Arc;

use eg_types::agent_graph::{
    AgentGraphCommittedResult, AgentGraphEntry, AgentGraphMutationKind, AgentGraphOutboxEvent,
    AgentGraphPublishRequest, AgentGraphRetireRequest, AgentGraphStatusRequest,
    AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
};
use eg_types::agent_library::AgentLibraryMutationContext;

use super::agent_library::AgentLibraryStore;
use super::agent_revision::{
    decode_revision, RevisionDefinition, RevisionLayer, RevisionTables, RevisionVerb,
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

/// The graph layer of the shared revision protocol.
pub(super) struct GraphLayer;

/// The graph layer's head and revision tables.
fn graph_tables() -> RevisionTables {
    RevisionTables {
        heads: eg_storage::AGENT_GRAPH_HEADS,
        revisions: eg_storage::AGENT_GRAPH_REVISIONS,
    }
}

impl RevisionLayer for GraphLayer {
    type Entry = AgentGraphEntry;
    type Kind = AgentGraphMutationKind;
    type Committed = AgentGraphCommittedResult;
    type WriteResult = AgentGraphWriteResult;

    const NOUN: &'static str = "agent graph";
    const RECORD: &'static str = "graph";
    const SLUG: &'static str = "agent-graph";
    const OPERATION: &'static str = "graph";
    const TOPIC: &'static str = AGENT_GRAPH_OUTBOX_TOPIC;
    const RESULT_SCHEMA_ID: &'static str = AGENT_GRAPH_RESULT_SCHEMA_ID;
    const SCHEMA_VERSION: u16 = AGENT_GRAPH_ENTRY_SCHEMA_VERSION;
    const ID_HEADER: &'static str = "graph_id";
    const RETIRE: AgentGraphMutationKind = AgentGraphMutationKind::Retire;
    const MAX_REVISIONS: usize = MAX_AGENT_GRAPH_REVISIONS;
    const MAX_HISTORY_BYTES: usize = MAX_AGENT_GRAPH_HISTORY_BYTES;

    fn revision_definition(graph: &AgentGraphEntry) -> RevisionDefinition<'_> {
        RevisionDefinition {
            tenant_id: &graph.tenant_id,
            record_id: &graph.graph_id,
            entry_revision: graph.entry_revision,
            lifecycle: graph.lifecycle,
            definition_digest: &graph.definition_digest,
            actor_scope: &graph.actor_scope,
            purpose_id: &graph.purpose_id,
            policy_digest: &graph.policy_digest,
        }
    }

    fn validate_entry(graph: &AgentGraphEntry) -> Result<(), String> {
        graph.validate()
    }

    fn verb(kind: AgentGraphMutationKind) -> RevisionVerb {
        match kind {
            AgentGraphMutationKind::Publish => RevisionVerb::Publish,
            AgentGraphMutationKind::Retire => RevisionVerb::Retire,
        }
    }

    fn encode_outbox_event(
        kind: AgentGraphMutationKind,
        graph: &AgentGraphEntry,
        context: &AgentLibraryMutationContext,
    ) -> Result<Vec<u8>, String> {
        let event = AgentGraphOutboxEvent {
            schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
            kind,
            graph: graph.clone(),
            performing_actor: context.caller_principal.clone(),
            action_actor_scope: context.actor_scope.clone(),
        };
        event.validate()?;
        eg_storage::encode_bounded(&event, "agent graph outbox event")
    }

    fn extend_outbox_headers(graph: &AgentGraphEntry, headers: &mut BTreeMap<String, String>) {
        headers.insert("shape_digest".to_string(), graph.shape_digest.clone());
        // The ceiling admission derived, on the receipt path. A consumer
        // reading the stream learns what this revision was admitted to cost
        // without re-resolving the composition tree.
        headers.insert(
            "composed_work_ceiling".to_string(),
            graph.composed_work_ceiling.to_string(),
        );
    }

    fn retired_revision(
        graph: &AgentGraphEntry,
        entry_revision: u64,
        retired_at_ms: u64,
    ) -> Result<AgentGraphEntry, String> {
        graph.retire(entry_revision, retired_at_ms)
    }

    fn committed(
        graph: AgentGraphEntry,
        batch_id: String,
        committed_version: u64,
    ) -> AgentGraphCommittedResult {
        AgentGraphCommittedResult {
            schema_version: AGENT_GRAPH_ENTRY_SCHEMA_VERSION,
            graph,
            batch_id,
            committed_version,
        }
    }

    fn committed_entry(result: &AgentGraphCommittedResult) -> &AgentGraphEntry {
        &result.graph
    }

    fn committed_binding(result: &AgentGraphCommittedResult) -> (&str, u64) {
        (&result.batch_id, result.committed_version)
    }

    fn write_result(result: AgentGraphCommittedResult, replayed: bool) -> AgentGraphWriteResult {
        AgentGraphWriteResult { result, replayed }
    }
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
        Ok(self
            .graph_revisions(tenant_id, graph_id)?
            .into_iter()
            .last())
    }

    /// Every retained revision of one graph, oldest first.
    pub fn graph_revisions(
        &self,
        tenant_id: &str,
        graph_id: &str,
    ) -> Result<Vec<AgentGraphEntry>, String> {
        super::agent_revision::revision_history::<GraphLayer>(
            self,
            graph_tables(),
            tenant_id,
            graph_id,
        )
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn graph_status(
        &self,
        request: AgentGraphStatusRequest,
    ) -> Result<Option<AgentGraphWriteResult>, String> {
        super::agent_revision::committed_status::<GraphLayer>(
            self,
            &request.context,
            &request.graph_id,
            request.kind,
        )
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
        let head_lifecycle = decode_revision::<GraphLayer>(head.value())?.lifecycle;

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
            let entry = decode_revision::<GraphLayer>(value.value())?;
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
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the graph surface.
pub type AgentGraphStoreRef = Arc<AgentLibraryStore>;

#[cfg(test)]
mod references;
#[cfg(test)]
pub(crate) mod tests;
