//! Agent template publish and retire.
//!
//! The template parent owns reads and instantiation, and the shared
//! [`super::super::agent_revision`] protocol owns replay admission, owner rows,
//! the typed receipt and the native commit. What is template-specific is the
//! entry a publish admits: its base agent's references resolved inside the
//! admitting write.

use super::super::agent_library::validate_context;
use super::super::agent_revision::{
    require_context_tenant, retire_revision, revision_scope, write_revision, RevisionWrite,
};
use super::*;

fn prepare_template_entry(
    store: &AgentLibraryStore,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentTemplatePublishRequest,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentTemplateEntry, String> {
    store.admit_entry_references_in_write(
        txn,
        &request.context.tenant_id,
        "agent template base",
        &request.template.base,
    )?;
    AgentTemplateEntry::create(
        request.template.clone(),
        next_revision,
        AgentLibraryLifecycle::Published,
        replay_context.created_at_ms,
        replay_context.created_at_ms,
    )
}

impl AgentLibraryStore {
    /// Publish the next template revision.
    pub fn publish_template(
        &self,
        request: AgentTemplatePublishRequest,
    ) -> Result<AgentTemplateWriteResult, String> {
        validate_context(self, &request.context)?;
        request.template.validate()?;
        require_context_tenant::<TemplateLayer>(&request.context, &request.template.tenant_id)?;
        let (expected_revision, owner) =
            revision_scope(self, &request.context, TemplateLayer::NOUN)?;
        let txn = self.mutations.open_write(&owner)?;
        let definition_digest =
            eg_types::agent_template::draft_definition_digest(&request.template);
        write_revision::<TemplateLayer>(
            self,
            txn,
            &owner,
            template_tables(),
            RevisionWrite {
                context: &request.context,
                kind: AgentTemplateMutationKind::Publish,
                record_id: &request.template.template_id,
                expected_revision,
                definition_digest: Some(&definition_digest),
            },
            |txn, next_revision, replay_context| {
                prepare_template_entry(self, txn, &request, next_revision, replay_context)
            },
        )
    }

    /// Retain the current template revision as a durable tombstone.
    pub fn retire_template(
        &self,
        request: AgentTemplateRetireRequest,
    ) -> Result<AgentTemplateWriteResult, String> {
        validate_context(self, &request.context)?;
        let (expected_revision, owner) =
            revision_scope(self, &request.context, TemplateLayer::NOUN)?;
        let txn = self.mutations.open_write(&owner)?;
        retire_revision::<TemplateLayer>(
            self,
            txn,
            &owner,
            template_tables(),
            &request.context,
            &request.template_id,
            expected_revision,
        )
    }
}
