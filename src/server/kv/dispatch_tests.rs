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
use eg_types::storage_wire::{KvCasOutcome, KvEntry, KvScanPage, KvWriteReceipt};
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

/// A KV request in `graph` under `scopes`, signed with an explicit attempt nonce and
/// stable operation key.
struct Signed<'a> {
    graph: &'a str,
    scopes: &'a [&'a str],
    nonce: &'a str,
    idempotency_key: &'a str,
}

fn signed(id: u64, how: &Signed<'_>, method: Method) -> Request {
    let mut request = Request {
        id,
        graph: how.graph.to_string(),
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
        scopes: how.scopes.iter().map(|scope| scope.to_string()).collect(),
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
            nonce: how.nonce,
            idempotency_key: how.idempotency_key,
        },
    );
    request
}

/// Sign two attempts with the same stable KV operation key. The request id and
/// nonce deliberately vary independently; the former is transport metadata
/// and the latter is the one-use attempt guard.
fn stable_kv_req(id: u64, nonce: &str, idempotency_key: &str, method: Method) -> Request {
    let how = Signed {
        graph: "__commons__",
        scopes: &["*"],
        nonce,
        idempotency_key,
    };
    signed(id, &how, method)
}

fn how<'a>(graph: &'a str, scopes: &'a [&'a str], nonce: &'a str) -> Signed<'a> {
    Signed {
        graph,
        scopes,
        nonce,
        idempotency_key: nonce,
    }
}

fn temp_state(tag: &str) -> (std::path::PathBuf, Arc<RwLock<ServerState>>) {
    let dir = std::env::temp_dir().join(format!("eg-kv-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let state = state_with_kv(&dir.to_string_lossy());
    (dir, state)
}

fn decode<T: serde::de::DeserializeOwned>(r: &crate::protocol::Response) -> T {
    match &r.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(bytes).expect("typed KV result"),
        other => panic!("expected a Raw KV result, got {other:?} / {:?}", r.error),
    }
}

fn put(key: &str, value: &[u8]) -> Method {
    Method::KvPut {
        namespace: "cfg".into(),
        key: key.into(),
        value: value.to_vec(),
    }
}

fn get(key: &str) -> Method {
    Method::KvGet {
        namespace: "cfg".into(),
        key: key.into(),
    }
}

fn cas(key: &str, expected_version: u64, new: Option<&[u8]>) -> Method {
    Method::KvCas {
        namespace: "cfg".into(),
        key: key.into(),
        expected_version,
        new: new.map(<[u8]>::to_vec),
    }
}

fn scan(prefix: &str, limit: usize, cursor: Option<String>) -> Method {
    Method::KvScan {
        namespace: "cfg".into(),
        prefix: prefix.into(),
        limit,
        cursor,
    }
}

// spec: EG-DURABLE-KERNEL-R102
// spec: EG-DURABLE-KERNEL-R102.1
// spec: EG-DURABLE-KERNEL-R102.3
#[tokio::test]
async fn kv_dispatch_put_get_scan_delete_cas() {
    let (dir, state) = temp_state("dispatch");
    let first: KvWriteReceipt = decode(&dispatch_on_heap(&state, req(1, put("k", b"v1"))).await);
    assert!(first.version > 0);

    let entry: Option<KvEntry> = decode(&dispatch_on_heap(&state, req(2, get("k"))).await);
    assert_eq!(
        entry,
        Some(KvEntry {
            value: b"v1".to_vec(),
            version: first.version
        })
    );
    let absent: Option<KvEntry> = decode(&dispatch_on_heap(&state, req(3, get("nope"))).await);
    assert_eq!(absent, None);

    // A CAS against a stale version refuses and reports the current one.
    let stale: KvCasOutcome =
        decode(&dispatch_on_heap(&state, req(4, cas("k", first.version + 100, Some(b"x")))).await);
    assert_eq!(
        stale,
        KvCasOutcome {
            swapped: false,
            version: first.version
        }
    );
    let swapped: KvCasOutcome =
        decode(&dispatch_on_heap(&state, req(5, cas("k", first.version, Some(b"v2")))).await);
    assert!(swapped.swapped && swapped.version > first.version);
    // Create-if-absent is a CAS against version 0.
    let created: KvCasOutcome =
        decode(&dispatch_on_heap(&state, req(6, cas("k2", 0, Some(b"two")))).await);
    assert!(created.swapped);
    let again: KvCasOutcome =
        decode(&dispatch_on_heap(&state, req(7, cas("k2", 0, Some(b"dup")))).await);
    assert!(!again.swapped);

    let page: KvScanPage = decode(&dispatch_on_heap(&state, req(8, scan("k", 0, None))).await);
    let keys: Vec<_> = page.entries.iter().map(|row| row.key.as_str()).collect();
    assert_eq!(keys, ["k", "k2"]);
    assert_eq!(page.entries[0].value, b"v2");
    assert_eq!(page.next_cursor, None);

    let r = dispatch_on_heap(
        &state,
        req(
            9,
            Method::KvDelete {
                namespace: "cfg".into(),
                key: "k2".into(),
            },
        ),
    )
    .await;
    assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
    // A re-create after delete gets a fresh, larger version: no A-B-A.
    let recreated: KvCasOutcome =
        decode(&dispatch_on_heap(&state, req(10, cas("k2", 0, Some(b"back")))).await);
    assert!(recreated.swapped && recreated.version > created.version);

    // Bad auth is rejected before routing.
    let mut bad = req(11, get("k"));
    bad.auth_token = "bogus".into();
    let r = dispatch_on_heap(&state, bad).await;
    assert_eq!(r.error.as_deref(), Some("AUTHENTICATION_REQUIRED"));
    assert_eq!(r.error_detail.as_deref(), Some("Authentication failed"));
    let _ = std::fs::remove_dir_all(&dir);
}

// spec: EG-DURABLE-KERNEL-R102.3
#[tokio::test]
async fn kv_scan_pages_are_bounded_and_resume_from_the_cursor() {
    let (dir, state) = temp_state("scan-pages");
    for i in 0..25u64 {
        let r = dispatch_on_heap(&state, req(100 + i, put(&format!("p/{i:02}"), b"v"))).await;
        assert!(r.error.is_none(), "{:?}", r.error);
    }
    dispatch_on_heap(&state, req(200, put("q/outside", b"v"))).await;
    let mut cursor = None;
    let mut seen = Vec::new();
    for page_no in 0..10u64 {
        let page: KvScanPage =
            decode(&dispatch_on_heap(&state, req(300 + page_no, scan("p/", 10, cursor))).await);
        assert!(page.entries.len() <= 10);
        seen.extend(page.entries.into_iter().map(|row| row.key));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    let expected: Vec<String> = (0..25).map(|i| format!("p/{i:02}")).collect();
    assert_eq!(seen, expected);
    // A cursor outside the prefix is refused.
    let r = dispatch_on_heap(&state, req(400, scan("p/", 10, Some("q/outside".into())))).await;
    assert!(r.error.is_some());
    let _ = std::fs::remove_dir_all(&dir);
}

// spec: EG-DURABLE-KERNEL-R102.2
#[tokio::test]
async fn kv_keys_are_scoped_by_graph_and_writes_need_kv_write() {
    let (dir, state) = temp_state("scope");
    let r = dispatch_on_heap(
        &state,
        signed(1, &how("__commons__", &["*"], "s1"), put("k", b"a")),
    )
    .await;
    assert!(r.error.is_none(), "{:?}", r.error);
    // The same tenant, principal, namespace and key in another graph is another row.
    let other: Option<KvEntry> = decode(
        &dispatch_on_heap(
            &state,
            signed(2, &how("kv-graph-b", &["*"], "s2"), get("k")),
        )
        .await,
    );
    assert_eq!(other, None);

    // A read-only grant reads but cannot write.
    let read_only = &["kv:read"][..];
    let seen: Option<KvEntry> = decode(
        &dispatch_on_heap(
            &state,
            signed(3, &how("__commons__", read_only, "s3"), get("k")),
        )
        .await,
    );
    assert!(seen.is_some());
    let denied = dispatch_on_heap(
        &state,
        signed(4, &how("__commons__", read_only, "s4"), put("k", b"b")),
    )
    .await;
    assert!(denied.result.is_none(), "kv:read must not write");
    assert!(
        format!("{:?}{:?}", denied.error, denied.error_detail).contains("ACCESS_DENIED"),
        "{:?} / {:?}",
        denied.error,
        denied.error_detail
    );
    let unchanged: Option<KvEntry> = decode(
        &dispatch_on_heap(
            &state,
            signed(5, &how("__commons__", &["*"], "s5"), get("k")),
        )
        .await,
    );
    assert_eq!(unchanged.map(|entry| entry.value), Some(b"a".to_vec()));
    let _ = std::fs::remove_dir_all(&dir);
}

// spec: EG-DURABLE-KERNEL-R102.4
#[tokio::test]
async fn kv_fresh_nonce_replays_stable_operation_without_duplicate_write() {
    let (dir, state) = temp_state("stable-replay");
    // Seed the compare-and-swap row with a separate operation. The retry pair
    // below then exercises the stored CAS verdict, rather than only replaying
    // an unconditional put.
    let seed: KvWriteReceipt =
        decode(&dispatch_on_heap(&state, req(99, put("stable", b"v0"))).await);
    let method = || cas("stable", seed.version, Some(b"v1"));

    let first: KvCasOutcome = decode(
        &dispatch_on_heap(
            &state,
            stable_kv_req(100, "kv-nonce-a", "kv-stable-operation", method()),
        )
        .await,
    );
    assert!(first.swapped);

    // A new transport attempt with the same verified operation key returns the
    // stored verdict, without another row/version/outbox append.
    let retry: KvCasOutcome = decode(
        &dispatch_on_heap(
            &state,
            stable_kv_req(101, "kv-nonce-b", "kv-stable-operation", method()),
        )
        .await,
    );
    assert_eq!(retry, first);

    // Reusing the consumed attempt nonce remains a refusal even though the
    // stable operation itself is replayable.
    let nonce_replay = dispatch_on_heap(
        &state,
        stable_kv_req(102, "kv-nonce-a", "kv-stable-operation", method()),
    )
    .await;
    assert!(nonce_replay.error.is_some(), "nonce reuse must be rejected");

    let read: Option<KvEntry> = decode(&dispatch_on_heap(&state, req(103, get("stable"))).await);
    assert_eq!(
        read,
        Some(KvEntry {
            value: b"v1".to_vec(),
            version: first.version
        })
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn percentile(sorted: &[std::time::Duration], pct: usize) -> std::time::Duration {
    sorted[(sorted.len() * pct / 100).min(sorted.len() - 1)]
}

/// Hot-path proof: served `KvPut`/`KvGet` through the real authenticated dispatch
/// against a durable (commit-before-ack) `kv.redb`, with p50/p99 budgets.
// spec: EG-DURABLE-KERNEL-R102.5
#[tokio::test]
async fn kv_served_hot_path_meets_latency_budgets() {
    const N: u64 = 400;
    let (dir, state) = temp_state("hot-path");
    let mut puts = Vec::new();
    let mut gets = Vec::new();
    for i in 0..N {
        let key = format!("hot/{i}");
        let request = req(1_000 + i, put(&key, &[7u8; 256]));
        let started = std::time::Instant::now();
        let r = dispatch_on_heap(&state, request).await;
        puts.push(started.elapsed());
        assert!(r.error.is_none(), "{:?}", r.error);
        let request = req(10_000 + i, get(&key));
        let started = std::time::Instant::now();
        let r = dispatch_on_heap(&state, request).await;
        gets.push(started.elapsed());
        assert!(matches!(r.result, Some(ResultPayload::Raw(_))));
    }
    puts.sort();
    gets.sort();
    let (put_p50, put_p99) = (percentile(&puts, 50), percentile(&puts, 99));
    let (get_p50, get_p99) = (percentile(&gets, 50), percentile(&gets, 99));
    println!(
        "kv served hot path (n={N}, 256 B values, durable): \
         KvPut p50={put_p50:?} p99={put_p99:?}; KvGet p50={get_p50:?} p99={get_p99:?}"
    );
    // Budgets (EG-DURABLE-KERNEL-R102.5), including envelope verification and the
    // durable commit; generous enough for a loaded shared CI runner.
    assert!(
        get_p50 < std::time::Duration::from_millis(5),
        "KvGet p50 {get_p50:?}"
    );
    assert!(
        get_p99 < std::time::Duration::from_millis(20),
        "KvGet p99 {get_p99:?}"
    );
    assert!(
        put_p50 < std::time::Duration::from_millis(25),
        "KvPut p50 {put_p50:?}"
    );
    assert!(
        put_p99 < std::time::Duration::from_millis(100),
        "KvPut p99 {put_p99:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
