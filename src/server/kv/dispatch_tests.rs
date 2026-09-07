//! Wire-level tests: the `Method::Kv*` ops through the real `dispatch`.

use crate::protocol::{Method, Request, ResultPayload};
use crate::server::{
    auth::{build_shared_test_request, dispatch_test_on_heap as dispatch_on_heap},
    ServerState,
};
use std::sync::Arc;
use tokio::sync::RwLock;

const SECRET: &str = "kv-test-secret";
const TEST_AGENT: &str = "unit-test-agent";

fn state_with_kv(dir: &str) -> Arc<RwLock<ServerState>> {
    let kv = Arc::new(super::KvStore::open(Some(dir)).unwrap());
    let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(TEST_AGENT));
    state.persist_dir = Some(dir.to_string());
    #[cfg(feature = "kv")]
    {
        state.kv = Some(kv);
    }
    #[cfg(not(feature = "kv"))]
    let _ = kv;
    Arc::new(RwLock::new(state))
}

fn req(id: u64, method: Method) -> Request {
    build_shared_test_request(SECRET, id, "__commons__", TEST_AGENT, method)
}

#[tokio::test]
async fn kv_dispatch_put_get_scan_delete_cas() {
    let dir = std::env::temp_dir().join(format!("eg-kv-dispatch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let state = state_with_kv(&dir.to_string_lossy());

    // KvPut → "ok"
    let r = dispatch_on_heap(
        &state,
        req(
            1,
            Method::KvPut {
                namespace: "cfg".into(),
                key: "k".into(),
                value: b"v1".to_vec(),
            },
        ),
    )
    .await;
    assert!(
        matches!(r.result, Some(ResultPayload::String(s)) if s == "ok"),
        "{:?}",
        r.error
    );

    // KvGet → the opaque bytes verbatim.
    let r = dispatch_on_heap(
        &state,
        req(
            2,
            Method::KvGet {
                namespace: "cfg".into(),
                key: "k".into(),
            },
        ),
    )
    .await;
    match r.result {
        Some(ResultPayload::Raw(v)) => assert_eq!(v, b"v1"),
        other => panic!("KvGet: {other:?} / {:?}", r.error),
    }

    // KvScan → ordered [(key, value)].
    dispatch_on_heap(
        &state,
        req(
            3,
            Method::KvPut {
                namespace: "cfg".into(),
                key: "k2".into(),
                value: b"v2".to_vec(),
            },
        ),
    )
    .await;
    let r = dispatch_on_heap(
        &state,
        req(
            4,
            Method::KvScan {
                namespace: "cfg".into(),
                prefix: "k".into(),
                limit: 0,
            },
        ),
    )
    .await;
    let pairs: Vec<(String, serde_bytes::ByteBuf)> = match r.result {
        Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
        other => panic!("KvScan: {other:?} / {:?}", r.error),
    };
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].0, "k");

    // KvCas → swaps only on match.
    let r = dispatch_on_heap(
        &state,
        req(
            5,
            Method::KvCas {
                namespace: "cfg".into(),
                key: "k".into(),
                expected: Some(b"v1".to_vec()),
                new: Some(b"V1".to_vec()),
            },
        ),
    )
    .await;
    assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

    // KvDelete → existed.
    let r = dispatch_on_heap(
        &state,
        req(
            6,
            Method::KvDelete {
                namespace: "cfg".into(),
                key: "k2".into(),
            },
        ),
    )
    .await;
    assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

    // Bad auth is rejected before routing.
    let mut bad = req(
        7,
        Method::KvGet {
            namespace: "cfg".into(),
            key: "k".into(),
        },
    );
    bad.auth_token = "bogus".into();
    let r = dispatch_on_heap(&state, bad).await;
    assert_eq!(r.error.as_deref(), Some("Authentication failed"));

    let _ = std::fs::remove_dir_all(&dir);
}
