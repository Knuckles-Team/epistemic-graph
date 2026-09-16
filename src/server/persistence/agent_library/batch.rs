//! Agent Library mutation batch construction.

use super::*;

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
    let headers = build_batch_headers(context, entry);
    let operations = agent_library_operations(kind, entry);
    let outbox = vec![MutationOutboxIntent {
        topic: AGENT_LIBRARY_OUTBOX_TOPIC.to_string(),
        key,
        payload: event_bytes,
        headers,
    }];
    let envelope = compile_batch_envelope(owner, context, &operations, &outbox)?;
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        envelope,
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

pub(super) fn build_batch_headers(
    context: &AgentLibraryMutationContext,
    entry: &AgentLibraryEntry,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), entry.tenant_id.clone()),
        ("agent_id".to_string(), entry.agent_id.clone()),
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
        ("actor".to_string(), context.caller_principal.clone()),
        (
            "action_actor_scope".to_string(),
            context.actor_scope.clone(),
        ),
        ("action_purpose_id".to_string(), context.purpose_id.clone()),
        (
            "action_policy_revision".to_string(),
            context.policy_revision.clone(),
        ),
        (
            "action_policy_digest".to_string(),
            context.policy_digest.clone(),
        ),
        (
            "action_policy_decision_id".to_string(),
            context.policy_decision_id.clone(),
        ),
        (
            "source_revision_digest".to_string(),
            entry.source_revision_digest.clone(),
        ),
    ])
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
