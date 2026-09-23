//! Durable Agent Component publish, retire and pin admission.
//!
//! The public store type remains in the parent module, and the shared
//! [`super::super::agent_revision`] protocol owns replay admission, owner rows,
//! the typed receipt and the native commit. What is component-specific is the
//! entry a publish admits: every component it pins resolved inside the
//! admitting write.

use super::super::agent_library::validate_context;
use super::super::agent_revision::{
    require_context_tenant, retire_revision, revision_scope, write_revision, RevisionWrite,
};
use super::*;

fn prepare_component_entry(
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    request: &AgentComponentPublishRequest,
    next_revision: u64,
    replay_context: &AgentLibraryMutationContext,
) -> Result<AgentComponentEntry, String> {
    super::super::decision_record::verify_policy_component(&request.component)?;
    super::pins::resolve_component_pins_in_write(
        txn,
        &request.context.tenant_id,
        "agent component",
        &request.component.pinned_components(),
    )?;
    AgentComponentEntry::create(
        request.component.clone(),
        next_revision,
        AgentLibraryLifecycle::Published,
        replay_context.created_at_ms,
        replay_context.created_at_ms,
    )
}

impl AgentLibraryStore {
    /// Publish the next component revision.
    pub fn publish_component(
        &self,
        request: AgentComponentPublishRequest,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &request.context)?;
        request.component.validate()?;
        require_context_tenant::<ComponentLayer>(&request.context, &request.component.tenant_id)?;
        super::super::decision_jobs::require_head_receipt(self, &request)?;
        let (expected_revision, owner) =
            revision_scope(self, &request.context, ComponentLayer::NOUN)?;
        let txn = self.mutations.open_write(&owner)?;
        write_revision::<ComponentLayer>(
            self,
            txn,
            &owner,
            component_tables(),
            RevisionWrite {
                context: &request.context,
                kind: AgentComponentMutationKind::Publish,
                record_id: &request.component.component_id,
                expected_revision,
                definition_digest: Some(&request.component.content_digest),
            },
            |txn, next_revision, replay_context| {
                prepare_component_entry(txn, &request, next_revision, replay_context)
            },
        )
    }

    /// Retain the current component revision as a durable tombstone.
    pub fn retire_component(
        &self,
        request: AgentComponentRetireRequest,
    ) -> Result<AgentComponentWriteResult, String> {
        validate_context(self, &request.context)?;
        let (expected_revision, owner) =
            revision_scope(self, &request.context, ComponentLayer::NOUN)?;
        let txn = self.mutations.open_write(&owner)?;
        retire_revision::<ComponentLayer>(
            self,
            txn,
            &owner,
            component_tables(),
            &request.context,
            &request.component_id,
            expected_revision,
        )
    }

    /// Resolve every pinned L1 component reference in the admitting write.
    pub(crate) fn resolve_component_pins_in_write(
        &self,
        write: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        tenant_id: &str,
        subject: &str,
        pins: &[&eg_types::agent_component::ComponentDependency],
    ) -> Result<(), String> {
        super::pins::resolve_component_pins_in_write(write, tenant_id, subject, pins)
    }
}
