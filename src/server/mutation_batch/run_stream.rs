//! Lifecycle payloads stay in private rows; only digest-bound references reach outbox.
use crate::protocol::Method;
use eg_types::run_stream::{
    RunEventReference, TerminalStreamReference, RUN_STREAM_TOPIC, TERMINAL_STREAM_TOPIC,
};
use eg_types::MutationOutboxIntent;

pub(super) fn lower_run_events(
    batch_id: &str,
    methods: &[Method],
) -> Result<Vec<MutationOutboxIntent>, String> {
    if let Some(Method::CommitWorkItemResult {
        work_item_id,
        terminal_stream: Some(stream),
        ..
    }) = methods.first()
    {
        if methods.len() != 1 || stream.run_id.trim().is_empty() || stream.run_id.len() > 512 {
            return Err("terminal stream requires one operation and bounded run identity".into());
        }
        let reference = TerminalStreamReference {
            work_item_id: work_item_id.clone(),
            run_id: stream.run_id.clone(),
            event_index: stream
                .expected_index
                .checked_add(1)
                .ok_or("terminal stream index exhausted")?,
        };
        return Ok(vec![MutationOutboxIntent {
            topic: TERMINAL_STREAM_TOPIC.into(),
            key: batch_id.into(),
            payload: rmp_serde::to_vec_named(&reference).map_err(|e| e.to_string())?,
            headers: Default::default(),
        }]);
    }
    let appends: Vec<_> = methods
        .iter()
        .filter_map(|method| match method {
            Method::AppendRunEvent { request } => Some(request),
            _ => None,
        })
        .collect();
    let Some(request) = appends.first() else {
        return Ok(vec![]);
    };
    if appends.len() != 1 || methods.len() != 1 {
        return Err("run event append requires one operation".into());
    }
    request.validate()?;
    let reference = RunEventReference {
        kind: request.event.kind.clone(),
        work_item_id: request.work_item_id.clone(),
        run_id: request.run_id.clone(),
        event_index: request
            .expected_index
            .checked_add(1)
            .ok_or("run event index exhausted")?,
        payload_digest: request.event.payload_digest.clone(),
    };
    Ok(vec![MutationOutboxIntent {
        topic: RUN_STREAM_TOPIC.into(),
        key: batch_id.into(),
        payload: rmp_serde::to_vec_named(&reference).map_err(|e| e.to_string())?,
        headers: Default::default(),
    }])
}
