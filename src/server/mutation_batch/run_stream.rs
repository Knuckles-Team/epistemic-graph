//! Lifecycle payloads stay in private rows; only digest-bound references reach outbox.
use crate::protocol::Method;
use eg_types::run_stream::{RunEventReference, RUN_STREAM_TOPIC};
use eg_types::MutationOutboxIntent;

pub(super) fn lower_run_events(
    batch_id: &str,
    methods: &[Method],
) -> Result<Vec<MutationOutboxIntent>, String> {
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
