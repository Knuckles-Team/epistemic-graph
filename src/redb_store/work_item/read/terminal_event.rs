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
    validate_receipt(scope, batch_id, receipt, row)?;
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
            work_item_id: Some(work_item_id.to_owned()),
            replay_cursor: Some(replay_cursor(
                scope,
                tenant,
                work_item_id,
                &event.run_id,
                row,
                digest,
            )?),
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

/// Verify that the outbox row is the exact member of the committed receipt.
fn validate_receipt(
    scope: &MutationScopeIdentity,
    batch_id: &str,
    receipt: &MutationBatchRecord,
    row: &MutationOutboxRecord,
) -> Result<(), String> {
    if receipt.status != MutationBatchStatus::Committed
        || &receipt.identity != scope
        || receipt.batch.batch_id != batch_id
        || receipt.batch.outbox.get(row.ordinal as usize) != Some(&row.intent)
        || receipt.committed_version != row.committed_version
    {
        return Err("terminal event is not bound to its committed receipt".into());
    }
    Ok(())
}

/// Bind an acknowledgement to the exact committed event. This is not a bearer
/// credential: readers must authorize the request and compare with the freshly
/// projected durable cursor, never trust a decoded caller-supplied offset.
fn replay_cursor(
    scope: &MutationScopeIdentity,
    tenant: &str,
    work_item_id: &str,
    run_id: &str,
    row: &MutationOutboxRecord,
    digest: &str,
) -> Result<String, String> {
    let identity = serde_json::to_vec(&(
        "eg/terminal-replay/v1",
        scope,
        tenant,
        work_item_id,
        run_id,
        &row.batch_id,
        row.commit_sequence,
        row.ordinal,
        digest,
    ))
    .map_err(|error| error.to_string())?;
    Ok(format!(
        "eg-terminal-v1:{}",
        hex::encode(Sha256::digest(identity))
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DURABLE-KERNEL-R028.2
    #[test]
    fn replay_cursor_binds_scope_task_run_and_committed_position() {
        let scope = MutationScopeIdentity::fixed_graph("tenant", "graph", "incarnation").unwrap();
        let row = MutationOutboxRecord {
            schema_version: 1,
            batch_id: "batch".into(),
            ordinal: 2,
            identity: scope.clone(),
            committed_version: eg_types::CommittedVersion::Graph {
                source: 6,
                target: 7,
            },
            commit_sequence: Some(7),
            intent: eg_types::MutationOutboxIntent {
                topic: RUN_EVENT_OUTBOX_TOPIC.into(),
                key: "task".into(),
                payload: vec![],
                headers: Default::default(),
            },
            created_at_ms: 1,
        };
        let cursor = replay_cursor(&scope, "tenant", "task", "run", &row, "digest").unwrap();
        assert_eq!(
            cursor,
            replay_cursor(&scope, "tenant", "task", "run", &row, "digest").unwrap()
        );
        for other in [
            replay_cursor(&scope, "other", "task", "run", &row, "digest"),
            replay_cursor(&scope, "tenant", "other", "run", &row, "digest"),
            replay_cursor(&scope, "tenant", "task", "other", &row, "digest"),
            replay_cursor(&scope, "tenant", "task", "run", &row, "other"),
        ] {
            assert_ne!(cursor, other.unwrap());
        }
        let other_scope =
            MutationScopeIdentity::fixed_graph("tenant", "graph", "new-incarnation").unwrap();
        assert_ne!(
            cursor,
            replay_cursor(&other_scope, "tenant", "task", "run", &row, "digest").unwrap()
        );
        let mut next = row.clone();
        next.commit_sequence = Some(8);
        assert_ne!(
            cursor,
            replay_cursor(&scope, "tenant", "task", "run", &next, "digest").unwrap()
        );
        next = row.clone();
        next.ordinal = 3;
        assert_ne!(
            cursor,
            replay_cursor(&scope, "tenant", "task", "run", &next, "digest").unwrap()
        );
        next = row.clone();
        next.batch_id = "other".into();
        assert_ne!(
            cursor,
            replay_cursor(&scope, "tenant", "task", "run", &next, "digest").unwrap()
        );
        next = row;
        next.created_at_ms = 999;
        assert_eq!(
            cursor,
            replay_cursor(&scope, "tenant", "task", "run", &next, "digest").unwrap()
        );
    }
}
