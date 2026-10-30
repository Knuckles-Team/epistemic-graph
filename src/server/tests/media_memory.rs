//! `MediaRemember` / `MediaRecall` through the FULL served dispatch chain
//! (EG-MEDIA-VOCAB-R650.3-R650.5).

use super::*;
use eg_types::media_memory::{MediaDerivation, MediaMemory, MediaRecall};

const PNG: &[u8] = &[0x89, b'P', b'N', b'G', 9, 8, 7, 6, 5];

fn remember(bytes: &[u8], mime: &str, derivations: &[(&str, &str)]) -> Method {
    Method::MediaRemember {
        bytes: bytes.to_vec(),
        mime: mime.into(),
        source_message: "msg-1".into(),
        citing_memory: "mem-1".into(),
        derivations: derivations
            .iter()
            .map(|(id, text)| MediaDerivation {
                derivation_id: (*id).into(),
                text: (*text).into(),
            })
            .collect(),
    }
}

fn recall_digest(blob_id: &str) -> Method {
    Method::MediaRecall {
        blob_id: Some(blob_id.into()),
        text: None,
    }
}

fn recall_text(text: &str) -> Method {
    Method::MediaRecall {
        blob_id: None,
        text: Some(text.into()),
    }
}

async fn send(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) -> Response {
    dispatch_on_heap(state, request(id, "__commons__", None, method)).await
}

fn body<T: serde::de::DeserializeOwned>(resp: &Response) -> T {
    match &resp.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).unwrap(),
        other => panic!("expected Raw result, got {other:?} / {:?}", resp.error),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// The served methods reach the one engine-state store: a remember through
/// dispatch is visible in `ServerState::media_memory` (the eg-modality store),
/// and the served recall answers exactly what that store answers.
// spec: EG-MEDIA-VOCAB-R650.3
#[tokio::test]
async fn served_media_methods_reach_the_engine_state_store() {
    let state = test_state();
    let resp = send(&state, 1, remember(PNG, "image/png", &[("tr-1", "a red chart")])).await;
    assert_ok(&resp);
    let memory: MediaMemory = body(&resp);
    assert_eq!(memory.blob_id, sha256_hex(PNG));
    assert_eq!(memory.transcript_ref.as_deref(), Some("tr-1"));
    assert_eq!(memory.source_message, "msg-1");

    // Written once by digest: a repeat keeps the first node.
    let again = send(&state, 2, remember(PNG, "image/png", &[])).await;
    assert_eq!(body::<MediaMemory>(&again).blob_id, memory.blob_id);

    let tenant = crate::server::authority_context::signed_test_claims("system").tenant;
    let store = state.read().await.media_memory.clone();
    let direct = store
        .lock()
        .recall_by_digest(&tenant, &memory.blob_id)
        .expect("the served write landed in the engine-state store");
    let served: Vec<MediaRecall> = body(&send(&state, 3, recall_text("RED")).await);
    assert_eq!(served, vec![direct]);
}

/// Each refusal carries its stable wire code and leaves the store unchanged.
// spec: EG-MEDIA-VOCAB-R650.4
#[tokio::test]
async fn oversize_and_mismatched_blobs_are_refused_without_state_change() {
    let state = test_state();
    let mismatch = send(&state, 10, remember(PNG, "image/jpeg", &[("d", "t")])).await;
    assert_eq!(mismatch.error.as_deref(), Some("MEDIA_MEMORY_TYPE_MISMATCH"));

    let mut big = PNG.to_vec();
    big.resize(eg_modality::media_memory::MEDIA_MEMORY_MAX_BYTES + 1, 0);
    let oversize = send(&state, 11, remember(&big, "image/png", &[("d", "t")])).await;
    assert_eq!(oversize.error.as_deref(), Some("MEDIA_MEMORY_OVERSIZE_BLOB"));

    for (id, blob) in [(12, sha256_hex(PNG)), (13, sha256_hex(&big))] {
        let rows: Vec<MediaRecall> = body(&send(&state, id, recall_digest(&blob)).await);
        assert!(rows.is_empty(), "a refused write must not be recallable");
    }
    let rows: Vec<MediaRecall> = body(&send(&state, 14, recall_text("t")).await);
    assert!(rows.is_empty(), "a refused write must not attach derivations");
}

/// The evidence (digest, size, type, derivation ids, citing memory) reads back
/// through a separately signed `MediaRecall` request, by digest and by text.
// spec: EG-MEDIA-VOCAB-R650.5
#[tokio::test]
async fn evidence_reads_back_through_a_separate_recall() {
    let state = test_state();
    let wav: Vec<u8> = b"RIFF\x24\x00\x00\x00WAVEfmt ".to_vec();
    let resp = send(
        &state,
        20,
        remember(&wav, "audio/wav", &[("tr-2", "call the plumber"), ("cap-1", "voice note")]),
    )
    .await;
    assert_ok(&resp);
    let memory: MediaMemory = body(&resp);

    let expected = MediaRecall {
        blob_id: sha256_hex(&wav),
        mime: "audio/wav".into(),
        size: wav.len() as u64,
        derivation_ids: vec!["cap-1".into(), "tr-2".into()],
        citing_memory: "mem-1".into(),
    };
    let by_digest: Vec<MediaRecall> = body(&send(&state, 21, recall_digest(&memory.blob_id)).await);
    assert_eq!(by_digest, vec![expected.clone()]);
    let by_text: Vec<MediaRecall> = body(&send(&state, 22, recall_text("PLUMBER")).await);
    assert_eq!(by_text, vec![expected]);

    let bad = send(
        &state,
        23,
        Method::MediaRecall {
            blob_id: None,
            text: None,
        },
    )
    .await;
    assert_eq!(bad.error.as_deref(), Some("INVALID_ARGUMENT"));
}
