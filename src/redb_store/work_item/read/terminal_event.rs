//! Resolve only an exact native terminal binding; never mint an event.
use eg_types::outcome_bundle::{RunEvent, RUN_EVENT_OUTBOX_TOPIC};
use eg_types::work_item_read::{CommittedTerminalEvent, TerminalEventRead};
use eg_types::{
    MutationBatchRecord, MutationBatchStatus, MutationOutboxRecord, MutationScopeIdentity,
};
use sha2::{Digest, Sha256};

pub(super) fn project(
    scope: &MutationScopeIdentity,
    tenant: &str,
    work_item_id: &str,
    batch_id: &str,
    digest: &str,
    receipt: &MutationBatchRecord,
    records: &[MutationOutboxRecord],
) -> Result<TerminalEventRead, String> {
    let mut events = records
        .iter()
        .filter(|row| row.intent.topic == RUN_EVENT_OUTBOX_TOPIC);
    let row = events.next().ok_or("committed terminal event is missing")?;
    if events.next().is_some() || &row.identity != scope || row.batch_id != batch_id {
        return Err("committed terminal event binding mismatch".into());
    }
    if receipt.status != MutationBatchStatus::Committed
        || &receipt.identity != scope
        || receipt.batch.batch_id != batch_id
        || receipt.batch.outbox.get(row.ordinal as usize) != Some(&row.intent)
        || receipt.committed_version != row.committed_version
    {
        return Err("terminal event is not bound to its committed receipt".into());
    }
    if hex::encode(Sha256::digest(&row.intent.payload)) != digest {
        return Err("committed terminal event digest mismatch".into());
    }
    let event: RunEvent = rmp_serde::from_slice(&row.intent.payload)
        .map_err(|_| "committed terminal event is malformed".to_string())?;
    event.validate()?;
    if event.work_item_id != work_item_id || event.outbox_id != batch_id {
        return Err("committed terminal event task mismatch".into());
    }
    let sequence = row
        .commit_sequence
        .filter(|value| *value > 0)
        .ok_or("committed terminal event sequence unavailable")?;
    if row
        .committed_version
        .target()
        .is_some_and(|target| target != sequence)
    {
        return Err("committed terminal event sequence mismatch".into());
    }
    Ok(TerminalEventRead::Available(Box::new(
        CommittedTerminalEvent {
            tenant: tenant.to_owned(),
            scope: row.identity.clone(),
            batch_id: row.batch_id.clone(),
            commit_sequence: sequence,
            ordinal: row.ordinal,
            run_id: event.run_id,
            kind: event.kind,
            outcome: event.outcome,
            result_ref: event.result_ref,
            payload_digest: digest.to_string(),
        },
    )))
}
