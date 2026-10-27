//! Owner-bound terminal-text selection from one native graph read transaction.
use super::*;
use eg_types::inline_history::*;
use eg_types::work_item_read::RunWorkItemView;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

type Snapshot<'a> = eg_storage::ScopedRead<'a, eg_storage::GraphShardOwner>;
type Row = serde_json::Map<String, serde_json::Value>;
const UNAVAILABLE: &str = "RUN_ADMISSION_UNAVAILABLE: completed history unavailable";

fn row(
    snapshot: &Snapshot<'_>,
    graph: &str,
    id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Row>, String> {
    snapshot
        .scoped_owner_table(NODES)?
        .get((graph, id))?
        .map(|v| decode_durable(&crypto.unseal(v.value())?))
        .transpose()
}

/// Selection is opt-in; historical point reads keep their old behavior.
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_selection(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    run: &str,
    actor: &str,
    request: &RunSelectionRequest,
    crypto: DurableCrypto<'_>,
    now_ms: u64,
) -> Result<Option<RunWorkItemView>, String> {
    eg_types::work_item_read::validate_work_item_get(tenant, run)?;
    let handle = shard.graph(graph)?;
    let snapshot = shard.read(&handle)?;
    let Some(mut view) =
        run_owner::read_admission_in(&snapshot, graph, tenant, run, actor, crypto)?
    else {
        return Ok(None);
    };
    let current =
        row(&snapshot, graph, &view.work_item.work_item_id, crypto)?.ok_or(UNAVAILABLE)?;
    if request.version != 1
        || request.worker_id.is_empty()
        || property_u64(&current, "attempt") != 1
        || !commit_work_item_lease_is_valid(
            &current,
            &request.worker_id,
            request.lease_epoch,
            request.fencing_token,
            now_ms,
        )
        || property_f64(&current, "lease_expires_at") <= now_ms as f64 / 1000.0
    {
        return Err(UNAVAILABLE.into());
    }
    // ANY current pointer, including null/malformed, prevents history fallback.
    if current.contains_key(INLINE_CHECKPOINT) {
        return Err("RUN_ADMISSION_UNAVAILABLE: current checkpoint restoration unsupported".into());
    }
    let admission = view.admission.conversation.as_ref().ok_or(UNAVAILABLE)?;
    admission
        .history_profile
        .as_ref()
        .ok_or(UNAVAILABLE)?
        .validate()?;
    let mut expected_revision = admission.expected_revision;
    let mut previous = admission.predecessor_run_id.clone();
    let latest_predecessor = previous.clone();
    let mut seen = BTreeSet::from([run.to_string()]);
    let mut turns = Vec::new();
    let mut references = Vec::new();
    let mut retained_bytes = 0usize;
    while let Some(prior_run) = previous {
        if turns.len() >= MAX_INLINE_HISTORY_TURNS || !seen.insert(prior_run.clone()) {
            return Err(UNAVAILABLE.into());
        }
        let prior =
            run_owner::read_admission_in(&snapshot, graph, tenant, &prior_run, actor, crypto)?
                .ok_or(UNAVAILABLE)?;
        let turn = prior.admission.conversation.as_ref().ok_or(UNAVAILABLE)?;
        let profile = turn.history_profile.as_ref().ok_or(UNAVAILABLE)?;
        profile.validate()?;
        if turn.context_id != admission.context_id
            || turn.expected_revision.checked_add(1) != Some(expected_revision)
        {
            return Err(UNAVAILABLE.into());
        }
        // read_admission_in already validated this own-turn against the SAME
        // snapshot. Reuse that projection rather than reopening private rows.
        let envelope: CompletedTextEnvelope =
            serde_json::from_str(prior.completed_checkpoint.as_deref().ok_or(UNAVAILABLE)?)
                .map_err(|_| UNAVAILABLE)?;
        let reference = prior
            .run_completion
            .as_ref()
            .and_then(|c| c.payload.completed_checkpoint.clone())
            .ok_or(UNAVAILABLE)?;
        for message in &envelope.messages {
            retained_bytes = retained_bytes
                .checked_add(message.text.len())
                .ok_or(UNAVAILABLE)?;
        }
        if retained_bytes > MAX_INLINE_HISTORY_BYTES {
            return Err(UNAVAILABLE.into());
        }
        turns.push(envelope.messages);
        references.push(reference);
        expected_revision = turn.expected_revision;
        previous = turn.predecessor_run_id.clone();
    }
    if expected_revision != 0 {
        return Err(UNAVAILABLE.into());
    }
    let completed_history = if let Some(prior_run) = latest_predecessor {
        turns.reverse();
        references.reverse();
        let envelope = CompletedTextEnvelope {
            version: 1,
            phase: "completed".into(),
            run_id: prior_run,
            messages: turns.into_iter().flatten().collect(),
        };
        let content = serde_json::to_string(&envelope).map_err(|e| e.to_string())?;
        if content.len() > MAX_INLINE_HISTORY_BYTES {
            return Err(UNAVAILABLE.into());
        }
        Some(content)
    } else {
        None
    };
    view.checkpoint_selection = Some(RunCheckpointSelection {
        version: 1,
        run_id: run.into(),
        work_item_id: view.work_item.work_item_id.clone(),
        context_id: admission.context_id.clone(),
        admission_revision: admission
            .expected_revision
            .checked_add(1)
            .ok_or(UNAVAILABLE)?,
        scope: snapshot.scope().clone(),
        snapshot_version: eg_transaction::version(&snapshot)?,
        current_run_checkpoint: None,
        completed_history,
        checkpoints: references,
    });
    Ok(Some(view))
}

/// Shared owner-only private content validator; callers already resolved ownership.
pub(super) fn completed_turn(
    snapshot: &Snapshot<'_>,
    graph: &str,
    tenant: &str,
    actor: &str,
    prior: &RunWorkItemView,
    crypto: DurableCrypto<'_>,
) -> Result<(CompletedTextEnvelope, InlineCheckpointRef), String> {
    let prior_run = prior.admission.run_id.as_deref().ok_or(UNAVAILABLE)?;
    let turn = prior.admission.conversation.as_ref().ok_or(UNAVAILABLE)?;
    let profile = turn.history_profile.as_ref().ok_or(UNAVAILABLE)?;
    profile.validate()?;
    let expected_revision = turn.expected_revision.checked_add(1).ok_or(UNAVAILABLE)?;
    let completion = prior.run_completion.as_ref().ok_or(UNAVAILABLE)?;
    if completion.payload.outcome != "succeeded"
        || completion.payload.completion.completed_checkpoint.is_some()
    {
        return Err(UNAVAILABLE.into());
    }
    let reference = completion
        .payload
        .completed_checkpoint
        .as_ref()
        .ok_or(UNAVAILABLE)?;
    if reference.revision != 1
        || reference.document_id != format!("inline-checkpoint:{}", prior.work_item.work_item_id)
    {
        return Err(UNAVAILABLE.into());
    }
    let prior_row =
        row(snapshot, graph, &prior.work_item.work_item_id, crypto)?.ok_or(UNAVAILABLE)?;
    if prior_row.get(INLINE_CHECKPOINT)
        != Some(&serde_json::to_value(reference).map_err(|e| e.to_string())?)
    {
        return Err(UNAVAILABLE.into());
    }
    let bytes = snapshot
        .scoped_owner_table(inline_history::INLINE_HISTORY)?
        .get((graph, reference.document_id.as_str()))?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
        .ok_or(UNAVAILABLE)?;
    let stored: inline_history::StoredCheckpoint = decode_durable(&bytes)?;
    if stored.node_type != "InlineRunCheckpoint"
        || stored.tenant != tenant
        || stored.graph != graph
        || stored.owner != actor
        || stored.run_id != prior_run
        || stored.work_item_id != prior.work_item.work_item_id
        || stored.context_id != turn.context_id
        || stored.admission_revision != expected_revision
        || stored.checkpoint != *reference
        || stored.batch_id != completion.payload.batch_id
        || stored.lease_epoch != property_u64(&prior_row, "lease_epoch")
        || stored.fencing_token != property_u64(&prior_row, "fencing_token")
        || stored.content.len() > MAX_INLINE_HISTORY_BYTES
        || content_digest(&stored.content) != reference.content_digest
    {
        return Err(UNAVAILABLE.into());
    }
    let envelope: CompletedTextEnvelope =
        serde_json::from_str(&stored.content).map_err(|_| UNAVAILABLE)?;
    if envelope.version != 1
        || envelope.phase != "completed"
        || envelope.run_id != prior_run
        || envelope.messages.len() != 2
        || envelope.messages[0].role != "user"
        || envelope.messages[1].role != "assistant"
        || content_digest(&envelope.messages[0].text) != profile.user_text_digest
        || envelope.messages[1].text.trim().is_empty()
    {
        return Err(UNAVAILABLE.into());
    }
    Ok((envelope, reference.clone()))
}

pub(crate) fn read_head(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    context: &str,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ConversationHeadView>, String> {
    eg_types::work_item_read::validate_work_item_get(tenant, context)?;
    let mut digest = Sha256::new();
    for field in ["conversation-head", tenant, context] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    let id = format!("conversation-head:{}", hex::encode(digest.finalize()));
    let handle = shard.graph(graph)?;
    let snapshot = shard.read(&handle)?;
    let Some(head) = row(&snapshot, graph, &id, crypto)? else {
        return Ok(None);
    };
    if actor.is_empty()
        || property_string(&head, "owner") != actor
        || property_string(&head, "tenant") != tenant
    {
        return Ok(None);
    }
    if property_string(&head, "node_type") != "ConversationHead"
        || property_string(&head, "graph") != graph
        || property_string(&head, "context_id") != context
        || property_string(&head, "schema_version") != "1"
    {
        return Err(UNAVAILABLE.into());
    }
    let run = property_string(&head, "run_id");
    let view = run_owner::read_admission_in(&snapshot, graph, tenant, run, actor, crypto)?
        .ok_or(UNAVAILABLE)?;
    let turn = view.admission.conversation.as_ref().ok_or(UNAVAILABLE)?;
    let revision = property_u64(&head, "revision");
    if turn.context_id != context || turn.expected_revision.checked_add(1) != Some(revision) {
        return Err(UNAVAILABLE.into());
    }
    Ok(Some(ConversationHeadView {
        version: 1,
        context_id: context.into(),
        revision,
        run_id: run.into(),
        scope: snapshot.scope().clone(),
    }))
}
