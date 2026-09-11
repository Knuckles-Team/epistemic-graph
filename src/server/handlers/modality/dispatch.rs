use eg_audio::AudioData;
use eg_document::DocumentData;
use eg_image::ImageData;
use eg_types::{ServedModalityIngestItem, ServedModalityKind, ServedModalityOp};
use eg_video::VideoData;

use super::{
    capabilities, collect_tombstones, delete, events, ingest, ingest_stream, lifecycle,
    native_predicate, native_query, query, stats, ModalityAuthority, MAX_INGEST_STREAM_ITEMS,
};
use crate::graph::GraphCore;
use crate::protocol::ResultPayload;

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
        ServedModalityOp::Authority => ResultPayload::raw(&authority.view()),
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
            if !(2..=MAX_INGEST_STREAM_ITEMS).contains(&items.len()) {
                return Err("modality ingest stream cardinality is outside bounds".to_string());
            }
            let outcomes = ingest_stream(core, authority, modality, items)?;
            ResultPayload::raw(&outcomes)
        }
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
        ServedModalityOp::Capabilities { modality } => capabilities_operation(modality),
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
