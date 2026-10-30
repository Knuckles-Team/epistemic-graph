//! Served multimodal memory (EG-MEDIA-VOCAB-R650.3).
//!
//! `Method::MediaRemember` and `Method::MediaRecall` reach the one
//! implementation, `eg_modality::media_memory::MediaMemoryStore`, held in
//! engine state (`ServerState::media_memory`). The tenant comes from the
//! verified request context, never from a caller field. A refusal carries a
//! stable wire code (R650.4) and is decided before the store changes.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::authority_context::VerifiedRequestContext;
use super::super::state::ServerState;
use eg_modality::media_memory::MediaMemoryRefusal;
use eg_types::result_contract::storage as storage_results;

fn refusal(req_id: u64, refusal: MediaMemoryRefusal) -> Response {
    let detail = match refusal {
        MediaMemoryRefusal::OversizeBlob => "blob exceeds the media memory size ceiling",
        MediaMemoryRefusal::TypeMismatch => "declared type differs from the sniffed type",
        MediaMemoryRefusal::UnknownBlob => "no remembered blob has that digest",
    };
    Response::err(req_id, format!("{}: {detail}", refusal.code()))
}

/// Handle one media memory method, or hand any other method back.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Result<Response, Method> {
    let tenant = verified_context.claims().tenant.clone();
    match method {
        Method::MediaRemember {
            bytes,
            mime,
            source_message,
            citing_memory,
            derivations,
        } => {
            let store = state.read().await.media_memory.clone();
            let remembered = store.lock().remember_with(
                &tenant,
                &bytes,
                &mime,
                &source_message,
                &citing_memory,
                &derivations,
            );
            Ok(match remembered {
                Ok(memory) => Response::ok(
                    req_id,
                    ResultPayload::of_ref::<storage_results::MediaRemember>(&memory),
                ),
                Err(r) => refusal(req_id, r),
            })
        }
        Method::MediaRecall { blob_id, text } => {
            let store = state.read().await.media_memory.clone();
            let rows = match (blob_id, text) {
                (Some(blob_id), None) => store
                    .lock()
                    .recall_by_digest(&tenant, &blob_id)
                    .into_iter()
                    .collect::<Vec<_>>(),
                (None, Some(text)) => store.lock().recall_by_text(&tenant, &text),
                _ => {
                    return Ok(Response::err(
                        req_id,
                        "INVALID_ARGUMENT: MediaRecall needs exactly one of blob_id or text",
                    ))
                }
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::of_ref::<storage_results::MediaRecall>(&rows),
            ))
        }
        other => Err(other),
    }
}
