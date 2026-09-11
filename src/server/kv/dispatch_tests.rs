//! Wire-level tests: the `Method::Kv*` ops through the real `dispatch`.

use crate::acl::RequestContextClaims;
use crate::protocol::{Method, Request, ResultPayload};
use crate::server::{
    auth::{
        build_shared_test_request, compute_verified_envelope_token,
        dispatch_test_on_heap as dispatch_on_heap, VerifiedEnvelopeParams,
    },
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

/// Sign two attempts with the same stable KV operation key. The request id and
/// nonce deliberately vary independently; the former is transport metadata
/// and the latter is the one-use attempt guard.
fn stable_kv_req(id: u64, nonce: &str, idempotency_key: &str, method: Method) -> Request {
    let mut request = Request {
        id,
        graph: "__commons__".to_string(),
        auth_token: String::new(),
        agent_id: Some(TEST_AGENT.to_string()),
        method,
    };
    let context = RequestContextClaims {
        principal: TEST_AGENT.to_string(),
        tenant: "tenant-shared".to_string(),
        audience: "epistemic-graph-test".to_string(),
        agent_id: TEST_AGENT.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: "policy-test".to_string(),
        delegation: Vec::new(),
        node: None,
        priority: None,
    };
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("test clock is after the Unix epoch")
        .as_secs();
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp,
            nonce,
            idempotency_key,
        },
    );
    request
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

#[tokio::test]
async fn kv_fresh_nonce_replays_stable_operation_without_duplicate_write() {
    let dir = std::env::temp_dir().join(format!(
        "eg-kv-dispatch-stable-replay-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let state = state_with_kv(&dir.to_string_lossy());
    // Seed the compare-and-swap row with a separate operation. The retry pair
    // below then exercises the stored CAS verdict, rather than only replaying
    // an unconditional put.
    let seed = dispatch_on_heap(
        &state,
        req(
            99,
            Method::KvPut {
                namespace: "cfg".into(),
                key: "stable".into(),
                value: b"v0".to_vec(),
            },
        ),
    )
    .await;
    assert!(matches!(seed.result, Some(ResultPayload::String(s)) if s == "ok"));

    let method = || Method::KvCas {
        namespace: "cfg".into(),
        key: "stable".into(),
        expected: Some(b"v0".to_vec()),
        new: Some(b"v1".to_vec()),
    };

    let first = dispatch_on_heap(
        &state,
        stable_kv_req(100, "kv-nonce-a", "kv-stable-operation", method()),
    )
    .await;
    assert!(matches!(first.result, Some(ResultPayload::Bool(true))));

    // A new transport attempt with the same verified operation key returns the
    // stored verdict, without another row/version/outbox append.
    let retry = dispatch_on_heap(
        &state,
        stable_kv_req(101, "kv-nonce-b", "kv-stable-operation", method()),
    )
    .await;
    assert!(matches!(retry.result, Some(ResultPayload::Bool(true))));

    // Reusing the consumed attempt nonce remains a refusal even though the
    // stable operation itself is replayable.
    let nonce_replay = dispatch_on_heap(
        &state,
        stable_kv_req(102, "kv-nonce-a", "kv-stable-operation", method()),
    )
    .await;
    assert!(nonce_replay.error.is_some(), "nonce reuse must be rejected");

    let read = dispatch_on_heap(
        &state,
        req(
            103,
            Method::KvGet {
                namespace: "cfg".into(),
                key: "stable".into(),
            },
        ),
    )
    .await;
    assert!(matches!(read.result, Some(ResultPayload::Raw(value)) if value == b"v1"));
    let _ = std::fs::remove_dir_all(&dir);
}
