//! Served WorkItem replay proof over the authenticated dispatch and Redb path.
//!
//! The lower-level Redb fixtures prove the kernel's nonce/idempotency rules,
//! while this target proves the caller contract that reaches that kernel: a
//! fresh authenticated nonce with the same stable envelope key replays one
//! submit or lease transition, exact nonce reuse is rejected, and a changed
//! payload under the same key is a conflict.

#![cfg(all(feature = "server", feature = "security", feature = "redb"))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

use eg_types::native_control::{
    NativeControlSchemaVersion, SubmitWorkItemRequest, SubmitWorkItemResult,
};
use epistemic_graph::acl::RequestContextClaims;
use epistemic_graph::epistemic_operations::{
    ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ClaimWorkItemResult, RequestContext,
    RequestContextAuthenticationMethod, RequestContextSchemaVersion,
};
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::persistence::{redb_backend::RedbBackend, PersistenceBackend};
use epistemic_graph::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};

const SECRET: &str = "adopt-workitem-authenticated-replay-secret";
const GRAPH: &str = "adoptworkitemauthreplay";
const TENANT: &str = "integration-test-tenant";
const WORK_ITEM: &str = "work-item-auth-replay";
const WORKER: &str = "worker-auth-replay";
const AUDIENCE: &str = "epistemic-graph-integration-tests";
const POLICY_VERSION: &str = "integration-test-policy-v1";

fn state_with(backend: Arc<dyn PersistenceBackend>, dir: String) -> test_support::SharedState {
    let mut registry = GraphRegistry::new();
    registry
        .create_graph(GRAPH, epistemic_graph::protocol::GraphType::Commons, None)
        .expect("create graph");
    test_support::state_with_registry(
        SECRET,
        common::current_isolation(),
        registry,
        Some(dir),
        Some(backend),
    )
}

fn request_context(request_id: &str, trace_id: &str) -> RequestContext {
    RequestContext {
        schema_version: RequestContextSchemaVersion::V2,
        request_id: request_id.to_string(),
        subject_id: common::TEST_AGENT.to_string(),
        tenant_id: TENANT.to_string(),
        agent_id: common::TEST_AGENT.to_string(),
        scopes: vec!["*".to_string()],
        audience: AUDIENCE.to_string(),
        authentication_method: RequestContextAuthenticationMethod::WorkloadIdentity,
        policy_version: POLICY_VERSION.to_string(),
        graph: GRAPH.to_string(),
        placement_epoch: Some(0),
        trace_id: trace_id.to_string(),
        issued_at_ms: 1_000,
        expires_at_ms: 61_000,
    }
}

fn submit_method() -> Method {
    Method::SubmitWorkItem {
        request: SubmitWorkItemRequest {
            schema_version: NativeControlSchemaVersion::V1,
            context: request_context("client-submit-request", "client-submit-trace"),
            work_item_id: Some(WORK_ITEM.to_string()),
            idempotency_key: "work-item-inner-key".to_string(),
            command_digest: "a".repeat(64),
            kind: "authenticated-replay-test".to_string(),
            priority: 0,
            depends_on: Vec::new(),
            input_ref: "input:authenticated-replay".to_string(),
            policy_digest: "b".repeat(64),
            catalog_digest: "c".repeat(64),
            model_digest: "d".repeat(64),
            max_attempts: 3,
            deadline_unix: None,
            metadata: BTreeMap::new(),
            provenance_refs: Vec::new(),
            max_tenant_in_flight: 64,
        },
    }
}

fn claim_method(worker: &str) -> Method {
    Method::ClaimWorkItem {
        request: ClaimWorkItemRequest {
            schema_version: ClaimWorkItemRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            work_item_id: Some(WORK_ITEM.to_string()),
            queue_ref: None,
            resource_class: None,
            fairness_group: None,
            worker_ref: worker.to_string(),
            now_ms: 1_000,
            lease_ms: 5_000,
            max_tenant_in_flight: 64,
        },
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_secs()
}

fn signed_request(id: u64, method: Method, idempotency_key: &str) -> Request {
    common::configure_authority();
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let context = RequestContextClaims {
        principal: common::TEST_AGENT.to_string(),
        tenant: TENANT.to_string(),
        audience: AUDIENCE.to_string(),
        agent_id: common::TEST_AGENT.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: POLICY_VERSION.to_string(),
        delegation: Vec::new(),
        node: None,
        priority: None,
    };
    let mut request = Request {
        id,
        graph: GRAPH.to_string(),
        auth_token: String::new(),
        agent_id: Some(common::TEST_AGENT.to_string()),
        method,
    };
    let nonce = format!(
        "client-wire-{}-{}",
        std::process::id(),
        NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: now_secs(),
            nonce: &nonce,
            idempotency_key,
        },
    );
    request
}

fn decode_raw<T: serde::de::DeserializeOwned>(response: &Response, label: &str) -> T {
    assert_eq!(
        response.error, None,
        "{label} returned an error: {response:?}"
    );
    let bytes = match response.result.as_ref() {
        Some(ResultPayload::Raw(bytes)) => bytes,
        other => panic!("{label} did not return a typed byte result: {other:?}"),
    };
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .unwrap_or_else(|error| panic!("decode {label} result: {error:?}"))
}

async fn reopen(dir: &str) -> RedbBackend {
    for remaining in (0..=100).rev() {
        match RedbBackend::open(dir.to_string(), 8192) {
            Ok(backend) => return backend,
            Err(error) if remaining > 0 => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let _ = error;
            }
            Err(error) => panic!("reopen durable tier: {error:?}"),
        }
    }
    unreachable!("bounded reopen loop must return or panic")
}

fn temp_dir(label: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after the epoch")
                .as_nanos()
        ))
        .to_string_lossy()
        .into_owned()
}

async fn assert_durable_row(
    backend: &RedbBackend,
    expected_status: &str,
    expected_worker: Option<&str>,
) {
    let blob = backend
        .read_node(GRAPH, WORK_ITEM)
        .await
        .expect("read durable WorkItem")
        .expect("WorkItem row must exist");
    let props: serde_json::Map<String, serde_json::Value> =
        rmp_serde::from_slice(&blob).expect("decode durable WorkItem row");
    assert_eq!(
        props.get("status").and_then(|value| value.as_str()),
        Some(expected_status)
    );
    assert_eq!(
        props
            .get("command_sequence")
            .and_then(|value| value.as_u64()),
        Some(1)
    );
    match expected_worker {
        Some(worker) => assert_eq!(
            props.get("lease_owner").and_then(|value| value.as_str()),
            Some(worker)
        ),
        None => assert!(props.get("lease_owner").is_none() || props["lease_owner"].is_null()),
    }
}

#[tokio::test]
async fn authenticated_submit_dispatch_replays_once_in_redb() {
    let dir = temp_dir("eg-workitem-auth-submit");
    let concrete = Arc::new(RedbBackend::open(dir.clone(), 8192).expect("open redb backend"));
    let backend: Arc<dyn PersistenceBackend> = concrete.clone();
    let state = state_with(backend, dir.clone());
    let first_request = signed_request(1, submit_method(), "work-submit-envelope-key");

    let first = test_support::dispatch(&state, first_request.clone()).await;
    let first_result: SubmitWorkItemResult = decode_raw(&first, "SubmitWorkItem(first)");
    assert!(first_result.created);
    assert!(!first_result.replayed);

    let exact = test_support::dispatch(&state, first_request).await;
    assert!(
        exact
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact authenticated retry must consume its nonce: {exact:?}"
    );

    let fresh = test_support::dispatch(
        &state,
        signed_request(2, submit_method(), "work-submit-envelope-key"),
    )
    .await;
    let replay: SubmitWorkItemResult = decode_raw(&fresh, "SubmitWorkItem(replay)");
    assert!(!replay.created);
    assert!(replay.replayed);
    assert_eq!(replay.work_item_id, WORK_ITEM);

    let mut changed = submit_method();
    if let Method::SubmitWorkItem { request } = &mut changed {
        request.command_digest = "e".repeat(64);
    }
    let conflict = test_support::dispatch(
        &state,
        signed_request(3, changed, "work-submit-envelope-key"),
    )
    .await;
    assert!(
        conflict
            .error
            .as_deref()
            .is_some_and(|error| error.contains("IDEMPOTENCY_CONFLICT")),
        "same key with changed submit payload must conflict: {conflict:?}"
    );

    drop(state);
    concrete.shutdown();
    drop(concrete);
    let reopened = reopen(&dir).await;
    assert_durable_row(&reopened, "ready", None).await;
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn authenticated_claim_dispatch_replays_once_in_redb() {
    let dir = temp_dir("eg-workitem-auth-claim");
    let concrete = Arc::new(RedbBackend::open(dir.clone(), 8192).expect("open redb backend"));
    let backend: Arc<dyn PersistenceBackend> = concrete.clone();
    let state = state_with(backend, dir.clone());

    let seed = test_support::dispatch(
        &state,
        signed_request(1, submit_method(), "work-claim-seed-envelope-key"),
    )
    .await;
    assert!(seed.error.is_none(), "seed submit failed: {seed:?}");

    let first_request = signed_request(2, claim_method(WORKER), "work-claim-envelope-key");
    let first = test_support::dispatch(&state, first_request.clone()).await;
    let first_result: ClaimWorkItemResult = decode_raw(&first, "ClaimWorkItem(first)");
    assert!(first_result.claimed);
    assert_eq!(first_result.lease_epoch, Some(1));
    assert_eq!(first_result.fencing_token, Some(1));

    let exact = test_support::dispatch(&state, first_request).await;
    assert!(
        exact
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact authenticated claim retry must consume its nonce: {exact:?}"
    );

    let fresh = test_support::dispatch(
        &state,
        signed_request(3, claim_method(WORKER), "work-claim-envelope-key"),
    )
    .await;
    let replay: ClaimWorkItemResult = decode_raw(&fresh, "ClaimWorkItem(replay)");
    assert!(replay.claimed);
    assert_eq!(replay.lease_epoch, first_result.lease_epoch);
    assert_eq!(replay.fencing_token, first_result.fencing_token);

    let conflict = test_support::dispatch(
        &state,
        signed_request(
            4,
            claim_method("different-worker"),
            "work-claim-envelope-key",
        ),
    )
    .await;
    assert!(
        conflict
            .error
            .as_deref()
            .is_some_and(|error| error.contains("IDEMPOTENCY_CONFLICT")),
        "same key with changed claim payload must conflict: {conflict:?}"
    );

    drop(state);
    concrete.shutdown();
    drop(concrete);
    let reopened = reopen(&dir).await;
    assert_durable_row(&reopened, "leased", Some(WORKER)).await;
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}
