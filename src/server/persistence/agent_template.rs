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
//! The revision protocol is the one [`super::agent_revision`] writes once for
//! components, graphs and templates; [`TemplateLayer`] is what this layer
//! supplies to it.

use std::sync::Arc;

use eg_types::agent_library::{AgentLibraryLifecycle, AgentLibraryMutationContext};
use eg_types::agent_template::{
    AgentTemplateCommittedResult, AgentTemplateEntry, AgentTemplateMutationKind,
    AgentTemplateOutboxEvent, AgentTemplatePublishRequest, AgentTemplateRetireRequest,
    AgentTemplateStatusRequest, AGENT_TEMPLATE_SCHEMA_VERSION,
};

use super::agent_library::AgentLibraryStore;
use super::agent_revision::{
    decode_revision, require_physical_key, RevisionDefinition, RevisionLayer, RevisionTables,
    RevisionVerb,
};

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

/// The template layer of the shared revision protocol.
pub(super) struct TemplateLayer;

/// The template layer's head and revision tables.
fn template_tables() -> RevisionTables {
    RevisionTables {
        heads: eg_storage::AGENT_TEMPLATE_HEADS,
        revisions: eg_storage::AGENT_TEMPLATE_REVISIONS,
    }
}

impl RevisionLayer for TemplateLayer {
    type Entry = AgentTemplateEntry;
    type Kind = AgentTemplateMutationKind;
    type Committed = AgentTemplateCommittedResult;
    type WriteResult = AgentTemplateWriteResult;

    const NOUN: &'static str = "agent template";
    const RECORD: &'static str = "template";
    const SLUG: &'static str = "agent-template";
    const OPERATION: &'static str = "template";
    const TOPIC: &'static str = AGENT_TEMPLATE_OUTBOX_TOPIC;
    const RESULT_SCHEMA_ID: &'static str = AGENT_TEMPLATE_RESULT_SCHEMA_ID;
    const SCHEMA_VERSION: u16 = AGENT_TEMPLATE_SCHEMA_VERSION;
    const ID_HEADER: &'static str = "template_id";
    const RETIRE: AgentTemplateMutationKind = AgentTemplateMutationKind::Retire;
    const MAX_REVISIONS: usize = MAX_AGENT_TEMPLATE_REVISIONS;
    const MAX_HISTORY_BYTES: usize = MAX_AGENT_TEMPLATE_HISTORY_BYTES;

    fn revision_definition(template: &AgentTemplateEntry) -> RevisionDefinition<'_> {
        RevisionDefinition {
            tenant_id: &template.tenant_id,
            record_id: &template.template_id,
            entry_revision: template.entry_revision,
            lifecycle: template.lifecycle,
            definition_digest: &template.definition_digest,
            actor_scope: &template.actor_scope,
            purpose_id: &template.purpose_id,
            policy_digest: &template.policy_digest,
        }
    }

    fn validate_entry(template: &AgentTemplateEntry) -> Result<(), String> {
        template.validate()
    }

    fn verb(kind: AgentTemplateMutationKind) -> RevisionVerb {
        match kind {
            AgentTemplateMutationKind::Publish => RevisionVerb::Publish,
            AgentTemplateMutationKind::Retire => RevisionVerb::Retire,
        }
    }

    fn encode_outbox_event(
        kind: AgentTemplateMutationKind,
        template: &AgentTemplateEntry,
        context: &AgentLibraryMutationContext,
    ) -> Result<Vec<u8>, String> {
        let event = AgentTemplateOutboxEvent {
            schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
            kind,
            template: template.clone(),
            performing_actor: context.caller_principal.clone(),
            action_actor_scope: context.actor_scope.clone(),
        };
        event.validate()?;
        eg_storage::encode_bounded(&event, "agent template outbox event")
    }

    fn retired_revision(
        template: &AgentTemplateEntry,
        entry_revision: u64,
        retired_at_ms: u64,
    ) -> Result<AgentTemplateEntry, String> {
        template.retire(entry_revision, retired_at_ms)
    }

    fn committed(
        template: AgentTemplateEntry,
        batch_id: String,
        committed_version: u64,
    ) -> AgentTemplateCommittedResult {
        AgentTemplateCommittedResult {
            schema_version: AGENT_TEMPLATE_SCHEMA_VERSION,
            template,
            batch_id,
            committed_version,
        }
    }

    fn committed_entry(result: &AgentTemplateCommittedResult) -> &AgentTemplateEntry {
        &result.template
    }

    fn committed_binding(result: &AgentTemplateCommittedResult) -> (&str, u64) {
        (&result.batch_id, result.committed_version)
    }

    fn write_result(
        result: AgentTemplateCommittedResult,
        replayed: bool,
    ) -> AgentTemplateWriteResult {
        AgentTemplateWriteResult { result, replayed }
    }
}

impl AgentLibraryStore {
    /// The head revision of one template, or `None` if it was never published.
    pub fn current_template(
        &self,
        tenant_id: &str,
        template_id: &str,
    ) -> Result<Option<AgentTemplateEntry>, String> {
        Ok(self
            .template_revisions(tenant_id, template_id)?
            .into_iter()
            .last())
    }

    /// Every retained revision of one template, oldest first.
    pub fn template_revisions(
        &self,
        tenant_id: &str,
        template_id: &str,
    ) -> Result<Vec<AgentTemplateEntry>, String> {
        super::agent_revision::revision_history::<TemplateLayer>(
            self,
            template_tables(),
            tenant_id,
            template_id,
        )
    }

    /// Resolve a prior attempt's durable outcome without re-committing it.
    pub fn template_status(
        &self,
        request: AgentTemplateStatusRequest,
    ) -> Result<Option<AgentTemplateWriteResult>, String> {
        super::agent_revision::committed_status::<TemplateLayer>(
            self,
            &request.context,
            &request.template_id,
            request.kind,
        )
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
        let head_lifecycle = decode_revision::<TemplateLayer>(head.value())?.lifecycle;
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
        let entry = decode_revision::<TemplateLayer>(pinned.value())?;
        require_physical_key::<TemplateLayer>(
            &entry,
            &request.tenant_id,
            &request.template_id,
            revision,
        )?;
        entry.instantiate(&request.agent_id, &request.bindings)
    }
}

/// `pub(super)` for [`super::agent_pin_resolution`], which resolves a pin
/// against this layer and therefore has to read this layer's rows.
pub(super) fn decode_template(bytes: &[u8]) -> Result<AgentTemplateEntry, String> {
    decode_revision::<TemplateLayer>(bytes)
}

/// The shared `Arc` type the server state holds. Re-exported so the handler does
/// not have to name the library store to reach the template surface.
pub type AgentTemplateStoreRef = Arc<AgentLibraryStore>;

#[cfg(test)]
mod tests;
