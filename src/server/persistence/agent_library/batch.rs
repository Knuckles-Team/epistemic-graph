//! Agent-hierarchy mutation batch construction.

use super::*;
use crate::server::persistence::agent_revision::{definition_headers, RevisionDefinition};

pub(super) fn build_batch(
    owner: &eg_storage::OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
    entry: &AgentLibraryEntry,
    version: u64,
    batch_id: &str,
    event_bytes: Vec<u8>,
) -> Result<MutationBatch, String> {
    let key = format!(
        "{}:{}:{}",
        entry.tenant_id, entry.agent_id, entry.entry_revision
    );
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_LIBRARY_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers: build_batch_headers(context, entry),
    }];
    native_lifecycle_batch(
        owner,
        context,
        batch_id,
        version,
        agent_library_operations(kind, entry),
        outbox,
    )
}

/// One native ControlPlane lifecycle batch over its final operations and outbox.
///
/// Shared by every agent layer. `version` is the native version the batch is
/// expected to apply over.
pub(in crate::server::persistence) fn native_lifecycle_batch(
    owner: &eg_storage::OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    batch_id: &str,
    version: u64,
    operations: Vec<MutationOperation>,
    outbox: Vec<MutationOutboxIntent>,
) -> Result<MutationBatch, String> {
    // The envelope is minted from the batch's FINAL operations and outbox --
    // `MutationBatch::validate` compares its canonical payload digest against
    // exactly these, so building it from anything earlier fails closed with
    // "mutation batch content does not match its envelope's canonical payload
    // digest".
    let envelope = compile_batch_envelope(owner, context, &operations, &outbox)?;
    let batch = MutationBatch::native(
        batch_id,
        envelope,
        owner.identity().clone(),
        version,
        (operations, outbox),
        context.created_at_ms,
    );
    batch.validate_write_budget()?;
    Ok(batch)
}

/// The action provenance an Agent Library outbox intent carries beside its
/// definition headers.
pub(super) struct ActionHeaders<'a> {
    pub(super) actor: &'a str,
    pub(super) actor_scope: &'a str,
    pub(super) purpose_id: &'a str,
    pub(super) policy_revision: &'a str,
    pub(super) policy_digest: &'a str,
    pub(super) policy_decision_id: &'a str,
}

impl<'a> ActionHeaders<'a> {
    pub(super) fn of_context(context: &'a AgentLibraryMutationContext) -> Self {
        Self {
            actor: &context.caller_principal,
            actor_scope: &context.actor_scope,
            purpose_id: &context.purpose_id,
            policy_revision: &context.policy_revision,
            policy_digest: &context.policy_digest,
            policy_decision_id: &context.policy_decision_id,
        }
    }

    pub(super) fn of_event(event: &'a AgentLibraryOutboxEvent) -> Self {
        Self {
            actor: &event.performing_actor,
            actor_scope: &event.action_actor_scope,
            purpose_id: &event.action_purpose_id,
            policy_revision: &event.action_policy_revision,
            policy_digest: &event.action_policy_digest,
            policy_decision_id: &event.action_policy_decision_id,
        }
    }
}

/// The outbox headers of one Agent Library revision: the definition headers
/// every layer carries, the action provenance, and the source revision digest.
pub(super) fn library_outbox_headers(
    entry: &AgentLibraryEntry,
    action: ActionHeaders<'_>,
) -> BTreeMap<String, String> {
    let mut headers = definition_headers(
        AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
        "agent_id",
        &RevisionDefinition {
            tenant_id: &entry.tenant_id,
            record_id: &entry.agent_id,
            entry_revision: entry.entry_revision,
            lifecycle: entry.lifecycle,
            definition_digest: &entry.definition_digest,
            actor_scope: &entry.actor_scope,
            purpose_id: &entry.purpose_id,
            policy_digest: &entry.policy_digest,
        },
    );
    headers.extend(
        [
            ("actor", action.actor),
            ("action_actor_scope", action.actor_scope),
            ("action_purpose_id", action.purpose_id),
            ("action_policy_revision", action.policy_revision),
            ("action_policy_digest", action.policy_digest),
            ("action_policy_decision_id", action.policy_decision_id),
            ("source_revision_digest", &entry.source_revision_digest),
        ]
        .map(|(header, value)| (header.to_string(), value.to_string())),
    );
    headers
}

pub(super) fn build_batch_headers(
    context: &AgentLibraryMutationContext,
    entry: &AgentLibraryEntry,
) -> BTreeMap<String, String> {
    library_outbox_headers(entry, ActionHeaders::of_context(context))
}

fn compile_batch_envelope(
    owner: &eg_storage::OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    operations: &[MutationOperation],
    outbox: &[MutationOutboxIntent],
) -> Result<MutationEnvelope, String> {
    let content = BatchContent {
        operations,
        outbox,
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
    compiled_envelope.policy_digest = parse_prefixed_digest(&context.policy_digest)?;
    compiled_envelope.policy_revision = context.policy_revision.clone();
    compiled_envelope.policy_decision_id = context.policy_decision_id.clone();
    MutationEnvelope::for_compiled_batch(compiled_envelope)
}
