// spec: EG-DURABLE-KERNEL-R030.1.3
//! Signed dispatch to native provenance. These tests need the redb server build;
//! they do not qualify deployment identity, current sessions, or host launch.
use super::*;
use crate::server::auth::{dispatch_test_on_heap, scoped_test_request_on, ScopedTestCaller};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::state::ServerState;
use eg_types::control_lease::ControlLeaseIssued;
use eg_types::host_start::{HostStartBinding, HostStartInterfaces, IssueHostStartGrantRequest};
use tokio::sync::RwLock;

const SECRET: &str = "host-start-contract-test-secret";
const ISSUER: &str = "host-start-test-issuer";
const TENANT: &str = "tenant-shared";
const GRAPH: &str = "host-start-test";

async fn call(
    state: &Arc<RwLock<ServerState>>,
    id: u64,
    key: &str,
    scopes: &[&str],
    method: Method,
) -> Response {
    let request = scoped_test_request_on(
        SECRET,
        id,
        GRAPH,
        method,
        ScopedTestCaller {
            principal: ISSUER,
            tenant: TENANT,
            scopes,
            nonce: &format!("host-start-nonce-{id}"),
            idempotency_key: key,
        },
    );
    dispatch_test_on_heap(state, request).await
}

fn decode(response: Response) -> ControlLeaseIssued {
    assert!(response.error.is_none(), "{:?}", response.error);
    match response.result.unwrap() {
        crate::protocol::ResultPayload::Json(value) => serde_json::from_value(value).unwrap(),
        crate::protocol::ResultPayload::Raw(bytes) => rmp_serde::from_slice(&bytes).unwrap(),
        other => panic!("unexpected host-start payload {other:?}"),
    }
}

async fn fixture() -> (
    tempfile::TempDir,
    Arc<RwLock<ServerState>>,
    Arc<RedbBackend>,
    IssueHostStartGrantRequest,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(ISSUER));
    state.persist_dir = Some(dir.path().to_string_lossy().into_owned());
    let backend =
        RedbBackend::open(dir.path().join("graphs").to_string_lossy().into_owned(), 64).unwrap();
    let backend = Arc::new(backend);
    state.persistence = Some(backend.clone());
    let state = Arc::new(RwLock::new(state));
    let create = scoped_test_request_on(
        SECRET,
        1,
        "__commons__",
        Method::CreateGraph {
            graph_name: GRAPH.into(),
            graph_type: crate::protocol::GraphType::Global,
        },
        ScopedTestCaller {
            principal: ISSUER,
            tenant: TENANT,
            scopes: &["*"],
            nonce: "host-create",
            idempotency_key: "host-create",
        },
    );
    let created = dispatch_test_on_heap(&state, create).await;
    assert!(created.error.is_none(), "{:?}", created.error);
    let now = crate::server::txn::now_ms();
    let request = IssueHostStartGrantRequest {
        tenant: TENANT.into(),
        lease_id: "host-grant".into(),
        binding: HostStartBinding {
            principal_ref: "beneficiary".into(),
            session_ref: "opaque-session".into(),
            workspace_ref: "workspace".into(),
            configuration_digest: "a".repeat(64),
            interfaces: HostStartInterfaces::Terminal,
            web_logs: false,
            policy_revision: "policy-1".into(),
            session_expires_at_ms: now + 60_000,
        },
        expires_at_ms: now + 60_000,
        idempotency_key: "host-issue".into(),
    };
    (dir, state, backend, request)
}

#[tokio::test]
async fn signed_issuer_is_stamped_and_retry_retains_attribution() {
    let _env = crate::crypto::acquire_test_env_read_lock().await;
    let (_dir, state, _backend, request) = fixture().await;
    let mut denied_request = request.clone();
    denied_request.idempotency_key = "denied-issue".into();
    let denied = call(
        &state,
        2,
        "denied-issue",
        &["lease:write"],
        Method::IssueHostStartGrant {
            request: denied_request,
        },
    )
    .await;
    assert!(denied.result.is_none(), "denial must not produce a result");
    let error = denied
        .error
        .as_deref()
        .expect("generic lease writer must be refused");
    assert!(error.contains("ACCESS_DENIED"), "{error}");
    assert!(error.contains("host:issue-start"), "{error}");
    let issued = decode(
        call(
            &state,
            3,
            "host-issue",
            &["host:issue-start"],
            Method::IssueHostStartGrant {
                request: request.clone(),
            },
        )
        .await,
    );
    let retry = decode(
        call(
            &state,
            4,
            "host-issue",
            &["host:issue-start"],
            Method::IssueHostStartGrant { request },
        )
        .await,
    );
    assert_eq!(issued, retry);
    let provenance = issued.lease.unwrap().host_start_provenance.unwrap();
    assert_eq!(
        provenance.issuer_ref,
        crate::server::mutation_batch::principal_fingerprint(ISSUER).unwrap()
    );
    assert_ne!(provenance.issuer_ref, ISSUER);
}

async fn authority_snapshot(backend: &RedbBackend) -> serde_json::Value {
    let version = backend.read_mutation_graph_version(GRAPH).await.unwrap();
    let own = backend
        .read_control_lease(GRAPH, TENANT, "host-grant")
        .await
        .unwrap();
    let foreign = backend
        .read_control_lease(GRAPH, "foreign-tenant", "host-grant")
        .await
        .unwrap();
    serde_json::to_value((version, own, foreign)).unwrap()
}

#[tokio::test]
async fn signed_wrong_tenant_and_forged_provenance_refuse_without_mutation() {
    let _env = crate::crypto::acquire_test_env_read_lock().await;
    let (_dir, state, backend, request) = fixture().await;
    let before = authority_snapshot(&backend).await;
    assert!(before[1].is_null() && before[2].is_null());

    let mut foreign = request.clone();
    foreign.tenant = "foreign-tenant".into();
    foreign.idempotency_key = "foreign-issue".into();
    let mut forged = request.lease_at(crate::server::txn::now_ms()).unwrap();
    forged.idempotency_key = "forged-issue".into();
    forged.grant.insert(
        "host_start_provenance".into(),
        serde_json::json!({"schema_version": 1, "issuer_ref": "forged-issuer"}),
    );
    for (id, key, method, reason) in [
        (
            20,
            "foreign-issue",
            Method::IssueHostStartGrant { request: foreign },
            "request tenant must match verified request tenant",
        ),
        (
            21,
            "forged-issue",
            Method::IssueControlLease { request: forged },
            "host.start is issued by IssueHostStartGrant only",
        ),
    ] {
        let denied = call(
            &state,
            id,
            key,
            &["host:issue-start", "lease:write"],
            method,
        )
        .await;
        assert!(denied.result.is_none(), "refusal must not return a grant");
        let error = denied
            .error
            .as_deref()
            .expect("signed invalid authority must refuse");
        assert!(
            error.contains("ACCESS_DENIED") && error.contains(reason),
            "{error}"
        );
        assert_eq!(authority_snapshot(&backend).await, before);
    }
    let issued = decode(
        call(
            &state,
            22,
            "host-issue",
            &["host:issue-start"],
            Method::IssueHostStartGrant { request },
        )
        .await,
    );
    assert_eq!(
        issued.outcome,
        eg_types::control_lease::ControlLeaseIssueOutcome::Issued
    );
    assert_eq!(
        issued
            .lease
            .unwrap()
            .host_start_provenance
            .unwrap()
            .issuer_ref,
        crate::server::mutation_batch::principal_fingerprint(ISSUER).unwrap()
    );
}
