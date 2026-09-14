use eg_audio::AudioData;
use eg_document::DocumentData;
use eg_image::ImageData;
use eg_types::{ServedModalityIngestItem, ServedModalityKind, ServedModalityOp};
use eg_video::VideoData;

use super::{
    capabilities, collect_tombstones, delete, events, ingest, ingest_stream, lifecycle,
    native_predicate, native_query, query, response, stats, ModalityAuthority,
    MAX_INGEST_STREAM_ITEMS,
};
use crate::graph::GraphCore;
use crate::protocol::ResultPayload;

fn validate_ingest_stream_request_cardinality(items: usize) -> Result<(), String> {
    if !(2..=MAX_INGEST_STREAM_ITEMS).contains(&items) {
        return Err("modality ingest stream cardinality is outside bounds".to_string());
    }
    Ok(())
}

// Every served modality operation selects one concrete runtime type. Keep this
// mapping exhaustive so a new wire modality cannot silently fall through.
macro_rules! dispatch_modality {
    ($modality:expr, $function:ident, $($arg:expr),* $(,)?) => {
        match $modality {
            ServedModalityKind::Document => $function::<DocumentData>($($arg),*),
            ServedModalityKind::Image => $function::<ImageData>($($arg),*),
            ServedModalityKind::Audio => $function::<AudioData>($($arg),*),
            ServedModalityKind::Video => $function::<VideoData>($($arg),*),
        }
    };
}

pub(super) fn handle(
    core: &GraphCore,
    authority: &ModalityAuthority,
    op: ServedModalityOp,
) -> Result<ResultPayload, String> {
    match op {
        ServedModalityOp::Authority => response::encode_authority(authority),
        op @ (ServedModalityOp::Ingest { .. }
        | ServedModalityOp::IngestStream { .. }
        | ServedModalityOp::Delete { .. }
        | ServedModalityOp::MoveToCold { .. }
        | ServedModalityOp::Restore { .. }
        | ServedModalityOp::CollectTombstones { .. }) => handle_mutation(core, authority, op),
        op @ (ServedModalityOp::Query { .. }
        | ServedModalityOp::NativeQuery { .. }
        | ServedModalityOp::Events { .. }
        | ServedModalityOp::Stats { .. }
        | ServedModalityOp::Capabilities { .. }) => handle_read(core, authority, op),
    }
}

fn handle_mutation(
    core: &GraphCore,
    authority: &ModalityAuthority,
    op: ServedModalityOp,
) -> Result<ResultPayload, String> {
    match op {
        ServedModalityOp::Ingest {
            modality,
            idempotency_ref,
            target_occurrence_id,
            expected_version,
            bundle_msgpack,
            source_bytes,
        } => ingest(
            core,
            authority,
            modality,
            ServedModalityIngestItem {
                idempotency_ref,
                target_occurrence_id,
                expected_version,
                bundle_msgpack,
                source_bytes,
            },
        ),
        ServedModalityOp::IngestStream { modality, items } => {
            validate_ingest_stream_request_cardinality(items.len())?;
            let outcomes = ingest_stream(core, authority, modality, items)?;
            response::encode_ingest_stream_result(outcomes)
        }
        ServedModalityOp::Delete {
            modality,
            idempotency_ref,
            occurrence_id,
            expected_version,
        } => dispatch_modality!(
            modality,
            delete,
            core,
            authority,
            modality,
            idempotency_ref,
            occurrence_id,
            expected_version,
        ),
        ServedModalityOp::MoveToCold {
            modality,
            occurrence_id,
        } => dispatch_modality!(
            modality,
            lifecycle,
            core,
            authority,
            modality,
            occurrence_id,
            false
        ),
        ServedModalityOp::Restore {
            modality,
            occurrence_id,
        } => dispatch_modality!(
            modality,
            lifecycle,
            core,
            authority,
            modality,
            occurrence_id,
            true
        ),
        ServedModalityOp::CollectTombstones {
            modality,
            through_event_sequence,
        } => dispatch_modality!(
            modality,
            collect_tombstones,
            core,
            authority,
            modality,
            through_event_sequence,
        ),
        _ => unreachable!("read operation reached mutation dispatcher"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_stream_request_accepts_safe_max_and_rejects_one_more() {
        assert!(validate_ingest_stream_request_cardinality(MAX_INGEST_STREAM_ITEMS).is_ok());
        assert!(validate_ingest_stream_request_cardinality(MAX_INGEST_STREAM_ITEMS + 1).is_err());
    }
}

fn handle_read(
    core: &GraphCore,
    authority: &ModalityAuthority,
    op: ServedModalityOp,
) -> Result<ResultPayload, String> {
    match op {
        ServedModalityOp::Query {
            modality,
            segment_kind,
            after_occurrence_id,
            limit,
            include_cold,
        } => dispatch_modality!(
            modality,
            query,
            core,
            authority,
            modality,
            segment_kind,
            after_occurrence_id,
            limit,
            include_cold,
        ),
        ServedModalityOp::NativeQuery {
            predicate,
            after_occurrence_id,
            limit,
            include_cold,
        } => {
            let (modality, predicate) = native_predicate(authority, predicate)?;
            dispatch_modality!(
                modality,
                native_query,
                core,
                authority,
                modality,
                predicate,
                after_occurrence_id,
                limit,
                include_cold,
            )
        }
        ServedModalityOp::Events {
            modality,
            after_sequence,
            limit,
        } => dispatch_modality!(
            modality,
            events,
            core,
            authority,
            modality,
            after_sequence,
            limit,
        ),
        ServedModalityOp::Stats { modality } => {
            dispatch_modality!(modality, stats, core, authority, modality)
        }
        ServedModalityOp::Capabilities { modality } => capabilities_operation(modality),
        _ => unreachable!("authority or mutation reached read dispatcher"),
    }
}

fn capabilities_operation(modality: ServedModalityKind) -> Result<ResultPayload, String> {
    match modality {
        ServedModalityKind::Document => capabilities::<DocumentData>(),
        ServedModalityKind::Image => capabilities::<ImageData>(),
        ServedModalityKind::Audio => capabilities::<AudioData>(),
        ServedModalityKind::Video => capabilities::<VideoData>(),
    }
}
