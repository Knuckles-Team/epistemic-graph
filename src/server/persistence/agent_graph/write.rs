//! Agent graph publish and retire.
//!
//! The graph parent owns reads and reference resolution, and the shared
//! [`super::super::agent_revision`] protocol owns replay admission, owner rows,
//! the typed receipt and the native commit. What is graph-specific is the entry
//! a publish admits: its references resolved and its composed work ceiling
//! derived, inside the admitting write.

use super::super::agent_library::validate_context;
use super::super::agent_revision::{
    require_context_tenant, retire_revision, revision_scope, write_revision, RevisionWrite,
};
use super::*;

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
        eg_types::agent_library::AgentLibraryLifecycle::Published,
        replay_context.created_at_ms,
        replay_context.created_at_ms,
    )
}

impl AgentLibraryStore {
    /// Publish the next graph revision.
    pub fn publish_graph(
        &self,
        request: AgentGraphPublishRequest,
    ) -> Result<AgentGraphWriteResult, String> {
        validate_context(self, &request.context)?;
        request.graph.validate()?;
        require_context_tenant::<GraphLayer>(&request.context, &request.graph.tenant_id)?;
        let (expected_revision, owner) = revision_scope(self, &request.context, GraphLayer::NOUN)?;
        let txn = self.mutations.open_write(&owner)?;
        let draft_digest = eg_types::agent_graph::draft_definition_digest(&request.graph);
        write_revision::<GraphLayer>(
            self,
            txn,
            &owner,
            graph_tables(),
            RevisionWrite {
                context: &request.context,
                kind: AgentGraphMutationKind::Publish,
                record_id: &request.graph.graph_id,
                expected_revision,
                definition_digest: Some(&draft_digest),
            },
            |txn, next_revision, replay_context| {
                prepare_graph_entry(self, txn, &request, next_revision, replay_context)
            },
        )
    }

    /// Retain the current graph revision as a durable tombstone.
    pub fn retire_graph(
        &self,
        request: AgentGraphRetireRequest,
    ) -> Result<AgentGraphWriteResult, String> {
        validate_context(self, &request.context)?;
        let (expected_revision, owner) = revision_scope(self, &request.context, GraphLayer::NOUN)?;
        let txn = self.mutations.open_write(&owner)?;
        retire_revision::<GraphLayer>(
            self,
            txn,
            &owner,
            graph_tables(),
            &request.context,
            &request.graph_id,
            expected_revision,
        )
    }
}
