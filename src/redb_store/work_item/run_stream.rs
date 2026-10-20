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
    {
        return Err("run event private binding mismatch".into());
    }
    stored.event.validate()?;
    let mut events = records
        .iter()
        .filter(|row| row.intent.topic == RUN_STREAM_TOPIC);
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
        || rmp_serde::from_slice::<RunEventReference>(&row.intent.payload)
            .map_err(|_| "invalid committed run event intent")?
            != stored.reference
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
    if after < end {
        for index in after + 1..=end {
            events.push(read_event(index)?);
        }
    }
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
        has_more: end < next_index,
        next_index,
    }))
}

#[cfg(test)]
mod tests;
