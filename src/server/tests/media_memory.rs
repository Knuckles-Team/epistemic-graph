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

/// The served state with the durable blob CAS and KV store installed, the way
/// `server_startup` composes them from a persist dir.
fn durable_state() -> Arc<RwLock<ServerState>> {
    let state = test_state();
    let dir = crate::server::unique_temp_dir("media-memory");
    let dir = dir.to_str().expect("utf-8 temp dir").to_string();
    {
        let mut s = state.try_write().expect("fresh state");
        s.kv = Some(Arc::new(crate::server::kv::KvStore::open(Some(&dir)).unwrap()));
        s.blob = Some(crate::server::handlers::blob::cursors_for_test(&dir));
    }
    state
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

/// The served methods reach the durable substrates: the bytes are in the blob
/// CAS under their digest, and the index row reads back through a recall.
// spec: EG-MEDIA-VOCAB-R650.3
#[tokio::test]
async fn served_media_methods_write_the_blob_cas_and_the_durable_index() {
    let state = durable_state();
    let resp = send(&state, 1, remember(PNG, "image/png", &[("tr-1", "a red chart")])).await;
    assert_ok(&resp);
    let memory: MediaMemory = body(&resp);
    assert_eq!(memory.blob_id, sha256_hex(PNG));
    assert_eq!(memory.transcript_ref.as_deref(), Some("tr-1"));
    assert_eq!(memory.source_message, "msg-1");

    // Written once by digest: a repeat keeps the first node.
    let again = send(&state, 2, remember(PNG, "image/png", &[])).await;
    assert_eq!(body::<MediaMemory>(&again).blob_id, memory.blob_id);

    let blob = state.read().await.blob.clone().expect("blob CAS");
    assert_eq!(
        blob.store.get_chunk(&memory.blob_id).unwrap().as_deref(),
        Some(PNG),
        "the media bytes are stored in the blob CAS under their digest"
    );
    let served: Vec<MediaRecall> = body(&send(&state, 3, recall_text("RED")).await);
    assert_eq!(served.len(), 1);
    assert_eq!(served[0].blob_id, memory.blob_id);
}

/// Tenant A can never recall tenant B's media, by digest or by text.
// spec: EG-MEDIA-VOCAB-R650.3
#[tokio::test]
async fn one_tenant_cannot_recall_another_tenants_media() {
    let state = durable_state();
    let as_tenant = |tenant: &str| {
        let context =
            crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant("agent", tenant);
        crate::server::access::CarrierAuthority::from_verified(&context).expect("carrier")
    };
    let (a, b) = (as_tenant("tenant-a"), as_tenant("tenant-b"));
    let call = |authority: &crate::server::access::CarrierAuthority, id: u64, method: Method| {
        let state = state.clone();
        let authority = authority.clone();
        async move {
            crate::server::handlers::media_memory::try_handle(&state, id, &authority, method)
                .await
                .expect("media method")
        }
    };
    let stored = call(&b, 30, remember(PNG, "image/png", &[("tr-b", "secret plan")])).await;
    assert_ok(&stored);
    let blob_id = body::<MediaMemory>(&stored).blob_id;

    let by_digest: Vec<MediaRecall> = body(&call(&a, 31, recall_digest(&blob_id)).await);
    assert!(by_digest.is_empty(), "tenant A must not recall B's blob by digest");
    let by_text: Vec<MediaRecall> = body(&call(&a, 32, recall_text("secret")).await);
    assert!(by_text.is_empty(), "tenant A must not recall B's blob by text");
    let own: Vec<MediaRecall> = body(&call(&b, 33, recall_text("secret")).await);
    assert_eq!(own.len(), 1, "tenant B still recalls its own media");
}

/// Each refusal carries its stable wire code and leaves the store unchanged.
// spec: EG-MEDIA-VOCAB-R650.4
#[tokio::test]
async fn oversize_and_mismatched_blobs_are_refused_without_state_change() {
    let state = durable_state();
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
    let blob = state.read().await.blob.clone().expect("blob CAS");
    assert!(
        blob.store.get_chunk(&sha256_hex(PNG)).unwrap().is_none(),
        "a refused write must not store bytes in the blob CAS"
    );
}

/// The evidence (digest, size, type, derivation ids, citing memory) reads back
/// through a separately signed `MediaRecall` request, by digest and by text.
// spec: EG-MEDIA-VOCAB-R650.5
#[tokio::test]
async fn evidence_reads_back_through_a_separate_recall() {
    let state = durable_state();
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
