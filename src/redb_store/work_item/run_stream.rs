//! Fenced lifecycle append and read-only replay through the existing mutation ledger.
use super::*;
use eg_types::run_stream::*;
use eg_types::{MutationBatch, MutationBatchRecord, MutationBatchStatus, MutationOutboxRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const RUN_EVENTS: redb::TableDefinition<(&str, &str, u64), &[u8]> =
    redb::TableDefinition::new("work_item_run_events");
type Row = serde_json::Map<String, serde_json::Value>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredEvent {
    #[serde(default)]
    terminal: bool,
    tenant: String,
    owner: String,
    batch_id: String,
    reference: RunEventReference,
    event: RunStreamPayload,
}

fn head(row: &Row) -> Result<u64, String> {
    match row.get(eg_types::work_item_read::WORK_ITEM_RUN_EVENT_INDEX) {
        None => Ok(0),
        Some(value) => value
            .as_u64()
            .filter(|n| *n > 0)
            .ok_or("invalid run event head".into()),
    }
}

pub(crate) fn is_owner(
    shard: &Shard,
    graph: &str,
    request: &GetRunEventsRequest,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<bool, String> {
    request.validate()?;
    if run_owner::work_item_id(graph, &request.tenant, &request.run_id)? != request.work_item_id {
        return Ok(false);
    }
    let (_, row) = read::snapshot_row(shard, graph, &request.work_item_id, crypto)?;
    Ok(row.is_some_and(|row| {
        run_owner::is_owned(
            &row,
            graph,
            &request.tenant,
            &request.run_id,
            &request.work_item_id,
            actor,
        )
    }))
}

pub(crate) fn append(
    graph: &str,
    request: &AppendRunEventRequest,
    batch: &MutationBatch,
    tables: &mut crate::redb_store::store_rows::GraphRowTables<'_>,
    crypto: DurableCrypto<'_>,
    now_ms: u64,
) -> Result<RunEventAppended, String> {
    request.validate()?;
    if batch.operations.len() != 1 {
        return Err("run event append requires one operation".into());
    }
    let actor = batch
        .envelope
        .operation()
        .map(|op| op.authority.actor.as_str())
        .unwrap_or_default();
    let mut row: Row = {
        let bytes = tables
            .nodes
            .get((graph, request.work_item_id.as_str()))?
            .ok_or("ACCESS_DENIED: run event owner unavailable")?;
        decode_durable(&crypto.unseal(bytes.value())?)?
    };
    if run_owner::work_item_id(graph, &request.tenant, &request.run_id)? != request.work_item_id
        || !run_owner::is_owned(
            &row,
            graph,
            &request.tenant,
            &request.run_id,
            &request.work_item_id,
            actor,
        )
    {
        return Err("ACCESS_DENIED: run event owner unavailable".into());
    }
    work_item_capability::require_native_claim(
        &tables.native_work_items,
        graph,
        &request.work_item_id,
        crypto,
    )?;
    if !run_owner::has_live_claim(
        &row,
        &request.worker_id,
        request.lease_epoch,
        request.fencing_token,
        now_ms,
    ) {
        return Err("run event live claim conflict".into());
    }
    if head(&row)? != request.expected_index {
        return Err("run event expected index conflict".into());
    }
    let event_index = request
        .expected_index
        .checked_add(1)
        .ok_or("run event index exhausted")?;
    let reference = RunEventReference {
        kind: request.event.kind.clone(),
        work_item_id: request.work_item_id.clone(),
        run_id: request.run_id.clone(),
        event_index,
        payload_digest: request.event.payload_digest.clone(),
    };
    let intents: Vec<_> = batch
        .outbox
        .iter()
        .filter(|intent| intent.topic == RUN_STREAM_TOPIC)
        .collect();
    if intents.len() != 1
        || intents[0].key != batch.batch_id
        || rmp_serde::from_slice::<RunEventReference>(&intents[0].payload)
            .map_err(|_| "invalid run event intent")?
            != reference
    {
        return Err("run event intent binding mismatch".into());
    }
    let key = (graph, request.work_item_id.as_str(), event_index);
    if tables.run_events.get(key)?.is_some() {
        return Err("immutable run event collision".into());
    }
    let stored = StoredEvent {
        terminal: false,
        tenant: request.tenant.clone(),
        owner: actor.to_owned(),
        batch_id: batch.batch_id.clone(),
        reference,
        event: request.event.clone(),
    };
    let bytes = rmp_serde::to_vec_named(&stored).map_err(|e| e.to_string())?;
    tables
        .run_events
        .insert(key, crypto.seal(&bytes).as_ref())?;
    row.insert(
        eg_types::work_item_read::WORK_ITEM_RUN_EVENT_INDEX.into(),
        event_index.into(),
    );
    row.insert("updated_at".into(), (now_ms as f64 / 1000.0).into());
    write_work_item_props(
        &mut tables.nodes,
        graph,
        &request.work_item_id,
        &mut row,
        crypto,
    )?;
    Ok(RunEventAppended {
        work_item_id: request.work_item_id.clone(),
        run_id: request.run_id.clone(),
        event_index,
        payload_digest: request.event.payload_digest.clone(),
        replayed: false,
        changed_work_item_ids: vec![request.work_item_id.clone()],
    })
}

fn project(
    stored: StoredEvent,
    request: &GetRunEventsRequest,
    actor: &str,
    event_index: u64,
    scope: &eg_types::MutationScopeIdentity,
    receipt: &MutationBatchRecord,
    records: &[MutationOutboxRecord],
) -> Result<CommittedRunStreamEvent, String> {
    if stored.tenant != request.tenant
        || stored.owner != actor
        || stored.reference.work_item_id != request.work_item_id
        || stored.reference.run_id != request.run_id
        || stored.reference.event_index != event_index
        || stored.reference.payload_digest != stored.event.payload_digest
        || stored.reference.kind != stored.event.kind
    {
        return Err("run event private binding mismatch".into());
    }
    stored.event.validate()?;
    let topic = if stored.terminal {
        TERMINAL_STREAM_TOPIC
    } else {
        RUN_STREAM_TOPIC
    };
    let mut events = records.iter().filter(|row| row.intent.topic == topic);
    let row = events.next().ok_or("committed run event missing")?;
    if events.next().is_some()
        || receipt.status != MutationBatchStatus::Committed
        || &receipt.identity != scope
        || &row.identity != scope
        || receipt.batch.batch_id != stored.batch_id
        || row.batch_id != stored.batch_id
        || row.intent.key != stored.batch_id
        || receipt.batch.outbox.get(row.ordinal as usize) != Some(&row.intent)
        || receipt.committed_version != row.committed_version
        || !reference_matches(&stored, &row.intent.payload, receipt)?
    {
        return Err("run event committed receipt mismatch".into());
    }
    let sequence = row
        .commit_sequence
        .filter(|n| *n > 0)
        .ok_or("run event commit sequence unavailable")?;
    if row
        .committed_version
        .target()
        .is_some_and(|target| target != sequence)
    {
        return Err("run event commit sequence mismatch".into());
    }
    let identity = serde_json::to_vec(&(
        "eg/run-event/v1",
        scope,
        &request.tenant,
        actor,
        &stored.reference,
        &row.batch_id,
        sequence,
        row.ordinal,
    ))
    .map_err(|e| e.to_string())?;
    let cursor = format!(
        "eg-run-v1:{event_index}:{}",
        hex::encode(Sha256::digest(identity))
    );
    Ok(CommittedRunStreamEvent {
        event_index,
        commit_sequence: sequence,
        ordinal: row.ordinal,
        cursor,
        event: stored.event,
    })
}

pub(crate) fn read_events(
    shard: &Shard,
    graph: &str,
    request: &GetRunEventsRequest,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<RunEventPage>, String> {
    request.validate()?;
    if run_owner::work_item_id(graph, &request.tenant, &request.run_id)? != request.work_item_id {
        return Ok(None);
    }
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    let row = nodes
        .get((graph, request.work_item_id.as_str()))?
        .map(|bytes| decode_durable::<Row>(&crypto.unseal(bytes.value())?))
        .transpose()?;
    let Some(row) = row else {
        return Ok(None);
    };
    if !run_owner::is_owned(
        &row,
        graph,
        &request.tenant,
        &request.run_id,
        &request.work_item_id,
        actor,
    ) {
        return Ok(None);
    }
    let next_index = head(&row)?;
    let table = read.scoped_owner_table(RUN_EVENTS)?;
    let read_event = |index| -> Result<CommittedRunStreamEvent, String> {
        let bytes = table
            .get((graph, request.work_item_id.as_str(), index))?
            .ok_or("run event payload missing")?;
        let stored: StoredEvent = decode_durable(&crypto.unseal(bytes.value())?)?;
        let receipt = eg_transaction::read_ledger(&read, &stored.batch_id)?
            .ok_or("run event receipt missing")?;
        let records = eg_transaction::read_outbox(&read, &stored.batch_id)?;
        project(
            stored,
            request,
            actor,
            index,
            read.scope(),
            &receipt,
            &records,
        )
    };
    let after = match &request.after_cursor {
        None => 0,
        Some(cursor) => {
            let index = cursor_index(cursor)?;
            if index > next_index || read_event(index)?.cursor != *cursor {
                return Err("invalid run event cursor binding".into());
            }
            index
        }
    };
    let end = after
        .saturating_add(u64::from(request.limit))
        .min(next_index);
    let mut events = Vec::new();
    let mut page_bytes = 0usize;
    if after < end {
        for index in after + 1..=end {
            let event = read_event(index)?;
            let bytes = serde_json::to_vec(&event).map_err(|e| e.to_string())?.len();
            if !events.is_empty() && page_bytes.saturating_add(bytes) > MAX_RUN_EVENT_PAGE_BYTES {
                break;
            }
            page_bytes = page_bytes.saturating_add(bytes);
            events.push(event);
        }
    }
    let last_index = events
        .last()
        .map(|event| event.event_index)
        .unwrap_or(after);
    let next_cursor = events
        .last()
        .map(|event| event.cursor.clone())
        .or_else(|| request.after_cursor.clone());
    let work_item = eg_types::work_item_read::WorkItemView::from_tenant_row(
        &request.work_item_id,
        &row,
        &request.tenant,
    )?
    .ok_or("run event WorkItem projection unavailable")?;
    Ok(Some(RunEventPage {
        work_item,
        events,
        next_cursor,
        has_more: last_index < next_index,
        next_index,
    }))
}

#[cfg(test)]
mod tests;

fn terminal_content(
    result: &crate::protocol::ResultPayload,
    work_item_id: &str,
) -> Result<Option<String>, String> {
    let crate::protocol::ResultPayload::Json(value) = result else {
        return Err("terminal stream result is not JSON".into());
    };
    let status = value
        .get("status")
        .and_then(serde_json::Value::as_str)
        .ok_or("terminal stream status missing")?;
    if matches!(status, "missing" | "noop" | "fenced") {
        return Ok(None);
    }
    if !matches!(
        status,
        "succeeded" | "failed" | "cancelled" | "dead_letter" | "retry_scheduled"
    ) || value
        .get("work_item_id")
        .and_then(serde_json::Value::as_str)
        != Some(work_item_id)
    {
        return Err("terminal stream result binding mismatch".into());
    }
    let state = if status == "retry_scheduled" {
        "ready"
    } else {
        status
    };
    serde_json::to_string(&serde_json::json!({"state": state}))
        .map(Some)
        .map_err(|e| e.to_string())
}
fn reference_matches(
    stored: &StoredEvent,
    payload: &[u8],
    receipt: &MutationBatchRecord,
) -> Result<bool, String> {
    if !stored.terminal {
        return Ok(rmp_serde::from_slice::<RunEventReference>(payload)
            .map_err(|_| "invalid committed event reference")?
            == stored.reference);
    }
    let reference: TerminalStreamReference =
        rmp_serde::from_slice(payload).map_err(|_| "invalid terminal event reference")?;
    let bytes = receipt
        .result_msgpack
        .as_deref()
        .ok_or("terminal event result missing")?;
    let result = rmp_serde::from_slice::<crate::protocol::ResultPayload>(bytes)
        .map_err(|_| "invalid terminal event receipt")?;
    Ok(reference.work_item_id == stored.reference.work_item_id
        && reference.run_id == stored.reference.run_id
        && reference.event_index == stored.reference.event_index
        && stored.event.kind == RunStreamKind::State
        && terminal_content(&result, &reference.work_item_id)?.as_deref()
            == Some(stored.event.content.as_str()))
}

/// Called after the native transition, before the SAME transaction is committed.
/// A refusal below rolls back the transition as well as all stream rows.
pub(crate) fn append_terminal(
    graph: &str,
    method: &Method,
    batch: &MutationBatch,
    result: &crate::protocol::ResultPayload,
    tables: &mut crate::redb_store::store_rows::GraphRowTables<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Method::CommitWorkItemResult {
        tenant,
        work_item_id,
        terminal_stream: Some(stream),
        ..
    } = method
    else {
        return Ok(());
    };
    let Some(content) = terminal_content(result, work_item_id)? else {
        return Ok(());
    };
    let actor = batch
        .envelope
        .operation()
        .map(|op| op.authority.actor.as_str())
        .unwrap_or_default();
    let mut row: Row = {
        let bytes = tables
            .nodes
            .get((graph, work_item_id.as_str()))?
            .ok_or("terminal stream WorkItem missing")?;
        decode_durable(&crypto.unseal(bytes.value())?)?
    };
    if run_owner::work_item_id(graph, tenant, &stream.run_id)? != *work_item_id
        || !run_owner::is_owned(&row, graph, tenant, &stream.run_id, work_item_id, actor)
    {
        return Err("ACCESS_DENIED: terminal stream owner unavailable".into());
    }
    work_item_capability::require_native_claim(
        &tables.native_work_items,
        graph,
        work_item_id,
        crypto,
    )?;
    let state: serde_json::Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;
    if state.get("state").and_then(serde_json::Value::as_str)
        != Some(property_string(&row, "status"))
    {
        return Err("terminal stream state disagrees with native WorkItem".into());
    }
    if head(&row)? != stream.expected_index {
        return Err("terminal stream expected index conflict".into());
    }
    let event_index = stream
        .expected_index
        .checked_add(1)
        .ok_or("terminal stream index exhausted")?;
    let expected = TerminalStreamReference {
        work_item_id: work_item_id.clone(),
        run_id: stream.run_id.clone(),
        event_index,
    };
    let mut intents = batch
        .outbox
        .iter()
        .filter(|intent| intent.topic == TERMINAL_STREAM_TOPIC);
    let intent = intents.next().ok_or("terminal stream intent missing")?;
    if intents.next().is_some()
        || intent.key != batch.batch_id
        || rmp_serde::from_slice::<TerminalStreamReference>(&intent.payload)
            .map_err(|_| "invalid terminal stream intent")?
            != expected
    {
        return Err("terminal stream intent binding mismatch".into());
    }
    let payload_digest = hex::encode(Sha256::digest(content.as_bytes()));
    let stored = StoredEvent {
        terminal: true,
        tenant: tenant.clone(),
        owner: actor.into(),
        batch_id: batch.batch_id.clone(),
        reference: RunEventReference {
            kind: RunStreamKind::State,
            work_item_id: work_item_id.clone(),
            run_id: stream.run_id.clone(),
            event_index,
            payload_digest: payload_digest.clone(),
        },
        event: RunStreamPayload {
            kind: RunStreamKind::State,
            content,
            payload_digest,
        },
    };
    let key = (graph, work_item_id.as_str(), event_index);
    if tables.run_events.get(key)?.is_some() {
        return Err("immutable terminal stream event collision".into());
    }
    let bytes = rmp_serde::to_vec_named(&stored).map_err(|e| e.to_string())?;
    tables
        .run_events
        .insert(key, crypto.seal(&bytes).as_ref())?;
    row.insert(
        eg_types::work_item_read::WORK_ITEM_RUN_EVENT_INDEX.into(),
        event_index.into(),
    );
    write_work_item_props(&mut tables.nodes, graph, work_item_id, &mut row, crypto)
}
