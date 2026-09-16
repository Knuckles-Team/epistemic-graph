use std::collections::BTreeMap;

use eg_storage::RecordedOperation;
use eg_transaction::{MutationKernel, ReplayResolution};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{MutationBatchStatus, MutationOutboxIntent};
use eg_types::{
    AgentLibraryCommittedResult, AgentLibraryEntryDraft, AgentLibraryLifecycle,
    AgentLibraryMutationContext, AgentLibraryMutationKind, AgentLibraryOutboxEvent,
    AgentLibraryWriteResult,
};

use super::receipt::agent_library_effect_digest;
use super::{
    agent_library_operations, batch_id, AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION,
    AGENT_LIBRARY_OUTBOX_TOPIC, AGENT_LIBRARY_RESULT_SCHEMA_ID,
};

/// What the caller believes its idempotency key committed.  A replayed receipt
/// is only safe to return when the recorded entry matches this expectation.
pub(super) struct ExpectedAgentLibraryMutation<'a> {
    pub(super) kind: AgentLibraryMutationKind,
    pub(super) agent_id: &'a str,
    /// `None` for a retire, which has no submitted draft to compare.
    pub(super) draft: Option<&'a AgentLibraryEntryDraft>,
}

struct ReplayEffect {
    event: AgentLibraryOutboxEvent,
    event_bytes: Vec<u8>,
    expected_key: String,
    headers: BTreeMap<String, String>,
}

/// What a recorded replay receipt claims was committed: the receipt, the
/// stable result decoded from it, the outbox effect rebuilt from that result,
/// and the lifecycle change. `validate_replay_evidence` checks the ledger
/// batch against all of it together.
struct ClaimedReplay<'r> {
    receipt: &'r MutationReceipt,
    stable_result: &'r AgentLibraryCommittedResult,
    effect: &'r ReplayEffect,
    kind: AgentLibraryMutationKind,
}

pub(super) fn replayed_receipt(
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    replay: ReplayResolution,
    operation: &eg_types::authority::OperationReplayIdentity,
    context: &AgentLibraryMutationContext,
    expected: ExpectedAgentLibraryMutation<'_>,
) -> Result<Option<(AgentLibraryWriteResult, MutationReceipt)>, String> {
    let Some(receipt) = recorded_replay_receipt(replay)? else {
        return Ok(None);
    };
    validate_replay_receipt_identity(&receipt, operation)?;
    let stable_result = receipt_result(&receipt)?;
    let kind = expected.kind;
    validate_replay_result(&stable_result, context, expected)?;
    let effect = build_replay_effect(&stable_result, context, kind)?;
    validate_replay_effect_digest(&receipt, &effect)?;
    validate_replay_evidence(
        mutations,
        txn,
        operation,
        context,
        &ClaimedReplay {
            receipt: &receipt,
            stable_result: &stable_result,
            effect: &effect,
            kind,
        },
    )?;
    Ok(Some((stable_result.response(true), receipt)))
}

fn recorded_replay_receipt(replay: ReplayResolution) -> Result<Option<MutationReceipt>, String> {
    let recorded = match replay {
        ReplayResolution::Fresh => return Ok(None),
        ReplayResolution::NonceRejected { idempotency_key } => {
            return Err(format!(
                "REPLAY_NONCE_CONSUMED: attempt nonce already consumed by '{idempotency_key}'"
            ));
        }
        ReplayResolution::Conflict { .. } => {
            return Err(
                "IDEMPOTENCY_CONFLICT: key was already used by a different Agent Library mutation"
                    .to_string(),
            );
        }
        ReplayResolution::ReplayedResult(recorded) => *recorded,
    };
    let RecordedOperation::Receipt(receipt) = recorded else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay is missing its typed receipt"
                .to_string(),
        );
    };
    Ok(Some(*receipt))
}

fn validate_replay_receipt_identity(
    receipt: &MutationReceipt,
    operation: &eg_types::authority::OperationReplayIdentity,
) -> Result<(), String> {
    receipt.validate()?;
    let operation_digest = operation.digest()?;
    if receipt.disposition.as_str() != "committed"
        || receipt.operation_replay_digest != operation_digest
        || receipt.scope != operation.authority_scope
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay receipt identity is invalid".to_string(),
        );
    }
    Ok(())
}

fn validate_replay_result(
    stable_result: &AgentLibraryCommittedResult,
    context: &AgentLibraryMutationContext,
    expected: ExpectedAgentLibraryMutation<'_>,
) -> Result<(), String> {
    let expected_batch_id = batch_id(&context.idempotency_key)?;
    let expected_entry_revision = context
        .expected_revision
        .and_then(|revision| revision.checked_add(1));
    let entry_matches = stable_result.batch_id == expected_batch_id
        && stable_result.entry.agent_id == expected.agent_id
        && stable_result.entry.tenant_id == context.tenant_id
        && expected_entry_revision == Some(stable_result.entry.entry_revision)
        && stable_result.entry.lifecycle == expected_lifecycle(expected.kind)
        && !expected
            .draft
            .is_some_and(|draft| stable_result.entry.as_draft() != *draft);
    if !entry_matches {
        return Err(
            "IDEMPOTENCY_CONFLICT: key was already used by a different Agent Library mutation"
                .to_string(),
        );
    }
    Ok(())
}

fn expected_lifecycle(kind: AgentLibraryMutationKind) -> AgentLibraryLifecycle {
    match kind {
        AgentLibraryMutationKind::Publish => AgentLibraryLifecycle::Published,
        AgentLibraryMutationKind::Retire => AgentLibraryLifecycle::Retired,
    }
}

fn build_replay_effect(
    stable_result: &AgentLibraryCommittedResult,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
) -> Result<ReplayEffect, String> {
    let event = AgentLibraryOutboxEvent::new(kind, stable_result.entry.clone(), context)?;
    let event_bytes = eg_storage::encode_bounded(&event, "agent library replay event")?;
    let expected_key = format!(
        "{}:{}:{}",
        stable_result.entry.tenant_id,
        stable_result.entry.agent_id,
        stable_result.entry.entry_revision
    );
    let headers = super::batch::build_batch_headers(context, &stable_result.entry);
    Ok(ReplayEffect {
        event,
        event_bytes,
        expected_key,
        headers,
    })
}

fn validate_replay_effect_digest(
    receipt: &MutationReceipt,
    effect: &ReplayEffect,
) -> Result<(), String> {
    let effect_digest = agent_library_effect_digest(
        AGENT_LIBRARY_OUTBOX_TOPIC,
        &effect.expected_key,
        &effect.event_bytes,
        &effect.headers,
    )?;
    if receipt.effect_digest != Some(effect_digest) {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay receipt does not bind its outbox"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_replay_evidence(
    mutations: &MutationKernel,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    operation: &eg_types::authority::OperationReplayIdentity,
    context: &AgentLibraryMutationContext,
    claimed: &ClaimedReplay<'_>,
) -> Result<(), String> {
    let ClaimedReplay {
        receipt,
        stable_result,
        effect,
        kind,
    } = *claimed;
    let Some((record, class, physical_outbox)) =
        mutations.read_replay_evidence(txn, &stable_result.batch_id)?
    else {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay receipt points to a missing batch"
                .to_string(),
        );
    };
    validate_replay_batch(&record, class, txn, context, stable_result)?;
    validate_replay_operation(&record, receipt, operation, stable_result, kind)?;
    validate_replay_intent(&record, effect)?;
    validate_replay_outbox(&record, &physical_outbox, txn)?;
    if record_event(&record)? != effect.event {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay event differs from its receipt"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_replay_batch(
    record: &eg_types::MutationBatchRecord,
    class: eg_storage::MutationClass,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    context: &AgentLibraryMutationContext,
    stable_result: &AgentLibraryCommittedResult,
) -> Result<(), String> {
    record.validate().map_err(|error| {
        format!("CORRUPT_MUTATION_LEDGER: Agent Library replay batch is invalid: {error}")
    })?;
    let valid = replay_batch_identity_matches(record, class, txn, stable_result)
        && replay_batch_context_matches(record, context)?;
    if !valid {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay batch identity is invalid".to_string(),
        );
    }
    Ok(())
}

fn replay_batch_identity_matches(
    record: &eg_types::MutationBatchRecord,
    class: eg_storage::MutationClass,
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
    stable_result: &AgentLibraryCommittedResult,
) -> bool {
    record.status == MutationBatchStatus::Committed
        && class == eg_storage::MutationClass::Operation
        && record.identity == *txn.scope()
        && record.batch.identity == *txn.scope()
        && record.batch.batch_id == stable_result.batch_id
        && record.batch.operations.len() == 1
        && record.batch.outbox.len() == 1
        && record.committed_version.target() == Some(stable_result.committed_version)
}

fn replay_batch_context_matches(
    record: &eg_types::MutationBatchRecord,
    context: &AgentLibraryMutationContext,
) -> Result<bool, String> {
    Ok(
        record.batch.idempotency_key() == context.idempotency_key.as_str()
            && record.batch.serving_principal() == context.principal.as_str()
            && record.committing_actor()? == context.caller_principal.as_str(),
    )
}

fn validate_replay_operation(
    record: &eg_types::MutationBatchRecord,
    receipt: &MutationReceipt,
    operation: &eg_types::authority::OperationReplayIdentity,
    stable_result: &AgentLibraryCommittedResult,
    kind: AgentLibraryMutationKind,
) -> Result<(), String> {
    let expected_operation = agent_library_operations(kind, &stable_result.entry)
        .into_iter()
        .next()
        .ok_or_else(|| {
            "CORRUPT_MUTATION_LEDGER: Agent Library replay operation is missing".to_string()
        })?;
    let actual_operation_bytes = eg_storage::encode_bounded(
        &record.batch.operations[0],
        "Agent Library replay operation",
    )?;
    let expected_operation_bytes =
        eg_storage::encode_bounded(&expected_operation, "Agent Library expected operation")?;
    if actual_operation_bytes != expected_operation_bytes {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay operation differs from its receipt"
                .to_string(),
        );
    }
    let receipt_result_bytes =
        eg_storage::encode_bounded(&receipt.result, "mutation receipt result")?;
    if record.result_msgpack.as_deref() != Some(receipt_result_bytes.as_slice()) {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay result bytes differ from its receipt"
                .to_string(),
        );
    }
    validate_replay_operation_identity(record, receipt, operation)
}

fn validate_replay_operation_identity(
    record: &eg_types::MutationBatchRecord,
    receipt: &MutationReceipt,
    operation: &eg_types::authority::OperationReplayIdentity,
) -> Result<(), String> {
    let physical_envelope = record.batch.envelope.operation().ok_or_else(|| {
        "CORRUPT_MUTATION_LEDGER: Agent Library replay batch is not an operation".to_string()
    })?;
    let physical_operation = physical_envelope.operation_identity()?;
    let mut comparable_operation = physical_operation;
    comparable_operation.canonical_payload_digest = operation.canonical_payload_digest;
    let physical_nonce = physical_envelope.nonce_replay_key()?;
    if comparable_operation != *operation || physical_nonce.digest()? != receipt.nonce_replay_digest
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay operation identity is invalid"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_replay_intent(
    record: &eg_types::MutationBatchRecord,
    effect: &ReplayEffect,
) -> Result<(), String> {
    let intent = &record.batch.outbox[0];
    if intent.topic != AGENT_LIBRARY_OUTBOX_TOPIC
        || intent.key != effect.expected_key
        || intent.payload != effect.event_bytes
        || intent.headers != effect.headers
    {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay batch outbox intent differs".to_string(),
        );
    }
    Ok(())
}

fn validate_replay_outbox(
    record: &eg_types::MutationBatchRecord,
    physical_outbox: &[eg_types::MutationOutboxRecord],
    txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
) -> Result<(), String> {
    if physical_outbox.len() != 1 {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay must contain one outbox row".to_string(),
        );
    }
    let outbox = &physical_outbox[0];
    outbox.validate().map_err(|error| {
        format!("CORRUPT_MUTATION_LEDGER: Agent Library replay outbox row is invalid: {error}")
    })?;
    let intent = &record.batch.outbox[0];
    let valid = outbox.batch_id == record.batch.batch_id
        && outbox.ordinal == 0
        && outbox.identity == *txn.scope()
        && outbox.committed_version == record.committed_version
        && outbox.created_at_ms == record.batch.created_at_ms
        && outbox.intent == *intent;
    if !valid {
        return Err(
            "CORRUPT_MUTATION_LEDGER: Agent Library replay outbox row differs from its batch"
                .to_string(),
        );
    }
    Ok(())
}

pub(super) fn receipt_result(
    receipt: &MutationReceipt,
) -> Result<AgentLibraryCommittedResult, String> {
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = &receipt.result
    else {
        return Err("Agent Library replay receipt has no DomainResult".to_string());
    };
    if schema_id.as_str() != AGENT_LIBRARY_RESULT_SCHEMA_ID {
        return Err("Agent Library replay receipt has an unexpected result schema".to_string());
    }
    let result: AgentLibraryCommittedResult =
        super::super::agent_row::decode(payload.as_slice(), "Agent Library replay result payload")?;
    result.validate()?;
    Ok(result)
}

struct ReplayRecordExpectation<'a> {
    context: &'a AgentLibraryMutationContext,
    batch_id: String,
    key: String,
    headers: BTreeMap<String, String>,
    event_bytes: &'a [u8],
}

pub(super) fn replay_record(
    record: &eg_types::MutationBatchRecord,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
    agent_id: &str,
    draft: Option<&AgentLibraryEntryDraft>,
    expected_event_bytes: &[u8],
) -> Result<AgentLibraryWriteResult, String> {
    let event = record_event(record)?;
    let stable_result = record_result(record)?;
    let intent = record
        .batch
        .outbox
        .first()
        .ok_or_else(|| "Agent Library receipt has no outbox event".to_string())?;
    let expected = ReplayRecordExpectation {
        context,
        batch_id: batch_id(&context.idempotency_key)?,
        key: format!(
            "{}:{}:{}",
            context.tenant_id, agent_id, event.entry.entry_revision
        ),
        headers: super::batch::build_batch_headers(context, &event.entry),
        event_bytes: expected_event_bytes,
    };
    let batch_matches = replay_record_batch_matches(record, intent, &stable_result, &expected)?;
    let event_matches = replay_record_event_matches(
        &event,
        &stable_result,
        expected.context,
        kind,
        agent_id,
        draft,
    );
    if !batch_matches || !event_matches {
        return Err(
            "IDEMPOTENCY_CONFLICT: key was already used by a different Agent Library mutation"
                .to_string(),
        );
    }
    if record.committed_version.target().is_none() {
        return Err("replayed Agent Library receipt has no target version".to_string());
    }
    Ok(stable_result.response(true))
}

fn replay_record_batch_matches(
    record: &eg_types::MutationBatchRecord,
    intent: &MutationOutboxIntent,
    stable_result: &AgentLibraryCommittedResult,
    expected: &ReplayRecordExpectation<'_>,
) -> Result<bool, String> {
    Ok(replay_record_batch_identity_matches(record, expected)?
        && replay_record_intent_matches(intent, expected)
        && replay_record_result_matches(record, stable_result, expected))
}

fn replay_record_batch_identity_matches(
    record: &eg_types::MutationBatchRecord,
    expected: &ReplayRecordExpectation<'_>,
) -> Result<bool, String> {
    Ok(record.status == MutationBatchStatus::Committed
        && record.validate_identity().is_ok()
        && record.batch.batch_id == expected.batch_id
        && record.batch.idempotency_key() == expected.context.idempotency_key
        && record.batch.identity.tenant().as_str() == expected.context.tenant_id
        && record.batch.serving_principal() == expected.context.principal
        && record.batch.operations.len() == 1
        && record.batch.outbox.len() == 1
        && record.committing_actor()? == expected.context.caller_principal.as_str())
}

fn replay_record_intent_matches(
    intent: &MutationOutboxIntent,
    expected: &ReplayRecordExpectation<'_>,
) -> bool {
    intent.topic == AGENT_LIBRARY_OUTBOX_TOPIC
        && intent.key == expected.key
        && intent.payload == expected.event_bytes
        && intent.headers == expected.headers
}

fn replay_record_result_matches(
    record: &eg_types::MutationBatchRecord,
    stable_result: &AgentLibraryCommittedResult,
    expected: &ReplayRecordExpectation<'_>,
) -> bool {
    stable_result.batch_id == expected.batch_id
        && stable_result.committed_version == record.committed_version.target().unwrap_or(0)
}

fn replay_record_event_matches(
    event: &AgentLibraryOutboxEvent,
    stable_result: &AgentLibraryCommittedResult,
    context: &AgentLibraryMutationContext,
    kind: AgentLibraryMutationKind,
    agent_id: &str,
    draft: Option<&AgentLibraryEntryDraft>,
) -> bool {
    replay_event_identity_matches(event, kind, agent_id, context)
        && replay_event_authority_matches(event, context)
        && replay_event_content_matches(event, stable_result, draft)
}

fn replay_event_identity_matches(
    event: &AgentLibraryOutboxEvent,
    kind: AgentLibraryMutationKind,
    agent_id: &str,
    context: &AgentLibraryMutationContext,
) -> bool {
    event.kind == kind
        && event.entry.agent_id == agent_id
        && event.entry.tenant_id == context.tenant_id
        && event.entry.lifecycle == expected_lifecycle(kind)
}

fn replay_event_authority_matches(
    event: &AgentLibraryOutboxEvent,
    context: &AgentLibraryMutationContext,
) -> bool {
    event.performing_actor == context.caller_principal
        && event.action_actor_scope == context.actor_scope
        && event.action_purpose_id == context.purpose_id
        && event.action_policy_revision == context.policy_revision
        && event.action_policy_digest == context.policy_digest
        && event.action_policy_decision_id == context.policy_decision_id
}

fn replay_event_content_matches(
    event: &AgentLibraryOutboxEvent,
    stable_result: &AgentLibraryCommittedResult,
    draft: Option<&AgentLibraryEntryDraft>,
) -> bool {
    !draft.is_some_and(|expected| event.entry.as_draft() != *expected)
        && stable_result.entry == event.entry
}

pub(super) fn expected_headers_from_event(
    event: &AgentLibraryOutboxEvent,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema_version".to_string(),
            AGENT_LIBRARY_OUTBOX_SCHEMA_VERSION.to_string(),
        ),
        ("tenant_id".to_string(), event.entry.tenant_id.clone()),
        ("agent_id".to_string(), event.entry.agent_id.clone()),
        (
            "entry_revision".to_string(),
            event.entry.entry_revision.to_string(),
        ),
        (
            "definition_digest".to_string(),
            event.entry.definition_digest.clone(),
        ),
        (
            "definition_actor_scope".to_string(),
            event.entry.actor_scope.clone(),
        ),
        (
            "definition_purpose_id".to_string(),
            event.entry.purpose_id.clone(),
        ),
        (
            "definition_policy_digest".to_string(),
            event.entry.policy_digest.clone(),
        ),
        ("actor".to_string(), event.performing_actor.clone()),
        (
            "action_actor_scope".to_string(),
            event.action_actor_scope.clone(),
        ),
        (
            "action_purpose_id".to_string(),
            event.action_purpose_id.clone(),
        ),
        (
            "action_policy_revision".to_string(),
            event.action_policy_revision.clone(),
        ),
        (
            "action_policy_digest".to_string(),
            event.action_policy_digest.clone(),
        ),
        (
            "action_policy_decision_id".to_string(),
            event.action_policy_decision_id.clone(),
        ),
        (
            "source_revision_digest".to_string(),
            event.entry.source_revision_digest.clone(),
        ),
    ])
}

pub(super) fn record_result(
    record: &eg_types::MutationBatchRecord,
) -> Result<AgentLibraryCommittedResult, String> {
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "Agent Library receipt has no typed domain result".to_string())?;
    let decoded = super::super::agent_row::try_decode::<MutationResult>(bytes);
    let mutation_result = match decoded {
        Ok(mutation_result) => mutation_result,
        // v6 Batch rows encoded the outbox event itself as result_msgpack.
        // Keep those rows readable for status/recovery projections, while the
        // nonce-first operation path remains Receipt-only and therefore cannot
        // silently treat a legacy row as replay authority.
        Err(_) => {
            let event: AgentLibraryOutboxEvent =
                super::super::agent_row::decode(bytes, "Agent Library domain result")?;
            event.validate()?;
            let committed_version = record
                .committed_version
                .target()
                .ok_or_else(|| "legacy Agent Library receipt has no target version".to_string())?;
            return AgentLibraryCommittedResult::new(
                event.entry,
                record.batch.batch_id.clone(),
                committed_version,
            );
        }
    };
    mutation_result.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = mutation_result
    else {
        return Err("Agent Library receipt does not contain a DomainResult".to_string());
    };
    if schema_id.as_str() != AGENT_LIBRARY_RESULT_SCHEMA_ID {
        return Err("Agent Library receipt has an unexpected result schema".to_string());
    }
    let result: AgentLibraryCommittedResult =
        super::super::agent_row::decode(payload.as_slice(), "Agent Library domain result payload")?;
    result.validate()?;
    Ok(result)
}

pub(super) fn record_event(
    record: &eg_types::MutationBatchRecord,
) -> Result<AgentLibraryOutboxEvent, String> {
    if record.batch.outbox.len() != 1 {
        return Err("Agent Library receipt must contain exactly one outbox event".to_string());
    }
    let intent = record
        .batch
        .outbox
        .first()
        .ok_or_else(|| "Agent Library receipt has no outbox event".to_string())?;
    let event: AgentLibraryOutboxEvent =
        super::super::agent_row::decode(intent.payload.as_slice(), "Agent Library outbox event")?;
    event.validate()?;
    Ok(event)
}
