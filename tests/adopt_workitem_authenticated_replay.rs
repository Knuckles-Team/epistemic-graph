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
            conversation: None,
            run_id: None,
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
        completed_text_history: false,
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
    signed_request_as(id, method, idempotency_key, common::TEST_AGENT)
}

fn signed_request_as(id: u64, method: Method, idempotency_key: &str, principal: &str) -> Request {
    signed_request_with_agent(id, method, idempotency_key, principal, principal)
}

fn signed_request_with_agent(
    id: u64,
    method: Method,
    idempotency_key: &str,
    principal: &str,
    agent: &str,
) -> Request {
    common::configure_authority();
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
    let context = RequestContextClaims {
        principal: principal.to_string(),
        tenant: TENANT.to_string(),
        audience: AUDIENCE.to_string(),
        agent_id: agent.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: POLICY_VERSION.to_string(),
        delegation: if principal == agent {
            Vec::new()
        } else {
            vec![principal.into(), agent.into()]
        },
        node: None,
        priority: None,
    };
    let mut request = Request {
        id,
        graph: GRAPH.to_string(),
        auth_token: String::new(),
        agent_id: Some(agent.to_string()),
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
            .is_some_and(|error| error == "REPLAY_NONCE_CONSUMED"),
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
        conflict.error.as_deref() == Some("IDEMPOTENCY_CONFLICT"),
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
            .is_some_and(|error| error == "REPLAY_NONCE_CONSUMED"),
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
        conflict.error.as_deref() == Some("IDEMPOTENCY_CONFLICT"),
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

fn run_submit_method() -> Method {
    let mut method = submit_method();
    if let Method::SubmitWorkItem { request } = &mut method {
        request.run_id = Some("run-owner-admission".into());
        request.context.agent_id = "agent:delegated-run-worker".into();
        request.work_item_id = None;
        request
            .metadata
            .insert("principal".into(), "forged-body-owner".into());
    }
    method
}

async fn run_state_with(
    backend: Arc<dyn PersistenceBackend>,
    dir: String,
) -> test_support::SharedState {
    use epistemic_graph::acl::{Grant, GrantEffect, RbacAction, ResourceSelector};

    let state = state_with(backend, dir);
    let mut isolation = test_support::wire_isolation(&[
        common::TEST_AGENT,
        "agent:delegated-run-worker",
        "agent:foreign-owner",
    ]);
    for action in [RbacAction::Read, RbacAction::Write] {
        isolation.add_grant(Grant {
            role: "commons-user".into(),
            resource: ResourceSelector::Graph(GRAPH.into()),
            action,
            effect: GrantEffect::Allow,
        });
    }
    state.write().await.isolation = isolation;
    state
}

async fn fresh_run_state(label: &str) -> (String, Arc<RedbBackend>, test_support::SharedState) {
    let dir = temp_dir(label);
    let concrete = Arc::new(RedbBackend::open(dir.clone(), 8192).unwrap());
    let state = run_state_with(concrete.clone(), dir.clone()).await;
    (dir, concrete, state)
}

async fn reopen_run_state(
    state: test_support::SharedState,
    concrete: Arc<RedbBackend>,
    dir: &str,
) -> (Arc<RedbBackend>, test_support::SharedState) {
    drop(state);
    concrete.shutdown();
    drop(concrete);
    let reopened = Arc::new(reopen(dir).await);
    let restarted = run_state_with(reopened.clone(), dir.to_owned()).await;
    // Restore both rows and their durable version, as a serving restart must.
    let (snapshot, version) = reopened
        .read_authoritative_graph_snapshot(GRAPH)
        .await
        .unwrap()
        .expect("durable run graph snapshot");
    restarted
        .read()
        .await
        .registry
        .get(GRAPH)
        .unwrap()
        .core
        .install_committed_snapshot(snapshot, version)
        .unwrap();
    (reopened, restarted)
}

fn signed_run_request(id: u64, method: Method, idempotency_key: &str) -> Request {
    signed_request_with_agent(
        id,
        method,
        idempotency_key,
        common::TEST_AGENT,
        "agent:delegated-run-worker",
    )
}

// spec: EG-DURABLE-KERNEL-R028.4
#[tokio::test]
async fn run_admission_binds_verified_owner_and_replays_without_row_changes() {
    use sha2::{Digest, Sha256};
    let (dir, concrete, state) = fresh_run_state("eg-run-owner-admission").await;
    let first = test_support::dispatch(
        &state,
        signed_request_with_agent(
            401,
            run_submit_method(),
            "run-admit-key",
            common::TEST_AGENT,
            "agent:delegated-run-worker",
        ),
    )
    .await;
    let admitted: SubmitWorkItemResult = decode_raw(&first, "run admission");
    let before = concrete
        .read_node(GRAPH, &admitted.work_item_id)
        .await
        .unwrap()
        .unwrap();
    let props: serde_json::Value = rmp_serde::from_slice(&before).unwrap();
    let binding = &props["run_owner_binding"];
    assert_eq!(binding["run_id"], "run-owner-admission");
    assert_eq!(binding["work_item_id"], admitted.work_item_id);
    assert_eq!(binding["graph"], GRAPH);
    assert_eq!(binding["tenant"], TENANT);
    assert_eq!(
        binding["principal"],
        format!(
            "principal:sha256:{}",
            hex::encode(Sha256::digest(common::TEST_AGENT.as_bytes()))
        )
    );
    let replay = test_support::dispatch(
        &state,
        signed_run_request(402, run_submit_method(), "run-admit-key"),
    )
    .await;
    let replay: SubmitWorkItemResult = decode_raw(&replay, "run admission replay");
    assert!(replay.replayed);
    assert_eq!(replay.work_item_id, admitted.work_item_id);
    assert_eq!(
        concrete
            .read_node(GRAPH, &admitted.work_item_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    let foreign = test_support::dispatch(
        &state,
        signed_request_with_agent(
            403,
            run_submit_method(),
            "run-admit-key",
            "agent:foreign-owner",
            "agent:delegated-run-worker",
        ),
    )
    .await;
    assert_eq!(
        foreign.error.as_deref(),
        Some("IDEMPOTENCY_CONFLICT"),
        "foreign owner receipt refusal: {foreign:?}"
    );
    for (index, case) in ["changed", "new-key", "attach", "tenant", "empty-run"]
        .into_iter()
        .enumerate()
    {
        let mut method = run_submit_method();
        if let Method::SubmitWorkItem { request } = &mut method {
            match case {
                "changed" => request.kind = "changed-effect-same-digest".into(),
                "new-key" => request.idempotency_key = "different-key".into(),
                "attach" => request.work_item_id = Some(admitted.work_item_id.clone()),
                "tenant" => request.context.tenant_id = "foreign-tenant".into(),
                "empty-run" => request.run_id = Some(String::new()),
                _ => unreachable!(),
            }
        }
        let refused = test_support::dispatch(
            &state,
            signed_run_request(410 + index as u64, method, "run-admit-key"),
        )
        .await;
        assert!(refused.error.is_some(), "{case} unexpectedly admitted");
        assert_eq!(
            concrete
                .read_node(GRAPH, &admitted.work_item_id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }
    let mut another = run_submit_method();
    if let Method::SubmitWorkItem { request } = &mut another {
        request.run_id = Some("another-run-same-key".into());
    }
    let another =
        test_support::dispatch(&state, signed_run_request(430, another, "run-admit-key")).await;
    let another: SubmitWorkItemResult = decode_raw(&another, "independent run admission");
    assert!(another.created);
    assert_ne!(another.work_item_id, admitted.work_item_id);
    let (reopened, restarted) = reopen_run_state(state, concrete, &dir).await;
    let replay = test_support::dispatch(
        &restarted,
        signed_run_request(440, run_submit_method(), "run-admit-key"),
    )
    .await;
    let replay: SubmitWorkItemResult = decode_raw(&replay, "reopened admission replay");
    assert!(replay.replayed);
    assert_eq!(replay.work_item_id, admitted.work_item_id);
    drop(restarted);
    assert_eq!(
        reopened
            .read_node(GRAPH, &admitted.work_item_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}

// spec: EG-DURABLE-KERNEL-R028.5
// spec: EG-DURABLE-KERNEL-R028.5.3
#[tokio::test]
async fn current_plan_native_publish_read_and_receipt_replay() {
    use eg_types::current_plan::{
        CurrentPlanPublished, CurrentPlanSnapshot, PlanItem, PlanStatus, PublishCurrentPlanRequest,
    };
    let dir = temp_dir("eg-current-plan-native");
    let concrete = Arc::new(RedbBackend::open(dir.clone(), 8192).unwrap());
    let state = run_state_with(concrete.clone(), dir.clone()).await;
    let admitted: SubmitWorkItemResult = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(501, run_submit_method(), "admit-plan"),
        )
        .await,
        "admit plan run",
    );
    let mut claim = claim_method(WORKER);
    if let Method::ClaimWorkItem { request, .. } = &mut claim {
        request.work_item_id = Some(admitted.work_item_id.clone());
        request.now_ms = now_secs() * 1000;
        request.lease_ms = 60_000;
    }
    let claimed: ClaimWorkItemResult = decode_raw(
        &test_support::dispatch(&state, signed_run_request(502, claim, "claim-plan")).await,
        "claim plan run",
    );
    assert!(claimed.claimed);
    let initial = PublishCurrentPlanRequest {
        tenant: TENANT.into(),
        worker_id: WORKER.into(),
        lease_epoch: claimed.lease_epoch.unwrap(),
        fencing_token: claimed.fencing_token.unwrap(),
        idempotency_key: "wire-key".into(),
        task_plan_proposal: None,
        snapshot: CurrentPlanSnapshot {
            run_id: "run-owner-admission".into(),
            revision: 0,
            observed_at_ms: now_secs() * 1000,
            expires_at_ms: now_secs() * 1000 + 30_000,
            items: vec![],
            task_plan: None,
        },
    };
    let publish = |request| Method::PublishCurrentPlan { request };
    let _: CurrentPlanPublished = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(503, publish(initial.clone()), "plan-zero"),
        )
        .await,
        "publish empty plan",
    );
    let mut updated = initial.clone();
    updated.snapshot.revision = 2;
    let attachment = eg_types::current_plan::sample_task_plan_publish_request();
    updated.snapshot.task_plan = attachment.snapshot.task_plan;
    updated.task_plan_proposal = attachment.task_plan_proposal;
    updated.snapshot.items = vec![
        PlanItem {
            text: "ordered first".into(),
            status: PlanStatus::Done,
        },
        PlanItem {
            text: "ordered second".into(),
            status: PlanStatus::InProgress,
        },
    ];
    let first: CurrentPlanPublished = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(504, publish(updated.clone()), "plan-two"),
        )
        .await,
        "publish updated plan",
    );
    let version_before = concrete.read_mutation_graph_version(GRAPH).await.unwrap();
    let receipt_id = current_plan_receipt_id("plan-two", 2);
    let outbox_before = serde_json::to_value(
        concrete
            .read_mutation_outbox(GRAPH, &receipt_id)
            .await
            .unwrap(),
    )
    .unwrap();
    let receipt_before = serde_json::to_value(
        concrete
            .read_mutation_batch(GRAPH, &receipt_id)
            .await
            .unwrap()
            .expect("publication has a durable receipt"),
    )
    .unwrap();
    let before = concrete
        .read_node(GRAPH, &admitted.work_item_id)
        .await
        .unwrap()
        .unwrap();
    let replay: CurrentPlanPublished = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(505, publish(updated.clone()), "plan-two"),
        )
        .await,
        "replay plan",
    );
    assert_eq!(first, replay);
    let get = || Method::GetCurrentPlan {
        tenant: TENANT.into(),
        run_id: initial.snapshot.run_id.clone(),
    };
    let read: Option<CurrentPlanSnapshot> = decode_raw(
        &test_support::dispatch(&state, signed_run_request(506, get(), "read-plan")).await,
        "read plan",
    );
    assert_eq!(read, Some(updated.snapshot.clone()));
    let foreign: Option<CurrentPlanSnapshot> = decode_raw(
        &test_support::dispatch(
            &state,
            signed_request_as(507, get(), "foreign-read", "agent:foreign-owner"),
        )
        .await,
        "foreign read",
    );
    assert!(foreign.is_none());
    let denied = test_support::dispatch(
        &state,
        signed_request_as(
            508,
            publish(updated.clone()),
            "plan-two",
            "agent:foreign-owner",
        ),
    )
    .await;
    assert!(denied.error.is_some());
    let mut changed = updated.clone();
    changed.task_plan_proposal.as_mut().unwrap().max_concurrency = 1;
    changed.snapshot.task_plan = Some(
        eg_types::decision::task_plan::plan_explicit_tasks(
            changed.task_plan_proposal.as_ref().unwrap(),
        )
        .unwrap(),
    );
    let conflict = test_support::dispatch(
        &state,
        signed_run_request(509, publish(changed.clone()), "plan-two"),
    )
    .await;
    assert!(conflict.error.is_some());
    changed.snapshot.revision = 3;
    let immutable_conflict = test_support::dispatch(
        &state,
        signed_run_request(513, publish(changed), "changed-task-plan"),
    )
    .await;
    assert!(immutable_conflict.error.is_some());
    let regression = test_support::dispatch(
        &state,
        signed_run_request(510, publish(updated.clone()), "new-key-same-revision"),
    )
    .await;
    assert!(regression.error.is_some());
    assert_eq!(
        concrete
            .read_node(GRAPH, &admitted.work_item_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    let after_refusal: Option<CurrentPlanSnapshot> = decode_raw(
        &test_support::dispatch(&state, signed_run_request(512, get(), "after-refusal-read")).await,
        "read after refusals",
    );
    assert_eq!(after_refusal, Some(updated.snapshot.clone()));
    assert_eq!(
        concrete.read_mutation_graph_version(GRAPH).await.unwrap(),
        version_before
    );
    assert_eq!(
        serde_json::to_value(
            concrete
                .read_mutation_outbox(GRAPH, &receipt_id)
                .await
                .unwrap()
        )
        .unwrap(),
        outbox_before
    );
    assert_eq!(
        serde_json::to_value(
            concrete
                .read_mutation_batch(GRAPH, &receipt_id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        receipt_before
    );
    let (reopened, restarted) = reopen_run_state(state, concrete, &dir).await;
    let recovered: Option<CurrentPlanSnapshot> = decode_raw(
        &test_support::dispatch(&restarted, signed_run_request(511, get(), "reopened-read")).await,
        "reopened plan",
    );
    assert_eq!(recovered, Some(updated.snapshot.clone()));
    assert_eq!(
        reopened
            .read_node(GRAPH, &admitted.work_item_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    current_plan_expired_replay(
        &restarted,
        &reopened,
        &admitted.work_item_id,
        updated,
        first,
    )
    .await;
    drop(restarted);
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}

// Locate the native receipt independently of the transport request number.
fn current_plan_receipt_id(key: &str, revision: u64) -> String {
    work_item_receipt_id(&[
        GRAPH,
        TENANT,
        key,
        "publish",
        "run-owner-admission",
        &revision.to_string(),
    ])
}

fn work_item_receipt_id(fields: &[&str]) -> String {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    hash.update(b"epistemic-graph.work-item-terminal.v1");
    for field in fields {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field.as_bytes());
    }
    format!("work:{}", hex::encode(hash.finalize()))
}

async fn current_plan_expired_replay(
    state: &test_support::SharedState,
    backend: &RedbBackend,
    work_item_id: &str,
    published: eg_types::current_plan::PublishCurrentPlanRequest,
    receipt: eg_types::current_plan::CurrentPlanPublished,
) {
    use eg_types::current_plan::{CurrentPlanPublished, CurrentPlanSnapshot};
    let renew = Method::RenewWorkItemLease {
        tenant: TENANT.into(),
        work_item_id: work_item_id.into(),
        worker_id: WORKER.into(),
        lease_epoch: published.lease_epoch,
        fencing_token: published.fencing_token,
        now_ms: now_secs() * 1000,
        lease_ms: 1,
    };
    let response =
        test_support::dispatch(state, signed_run_request(520, renew, "expire-plan-lease")).await;
    assert!(
        response.error.is_none(),
        "renew to short lease: {response:?}"
    );
    assert!(
        matches!(response.result, Some(ResultPayload::Json(ref value)) if value["renewed"] == true)
    );
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let version = backend.read_mutation_graph_version(GRAPH).await.unwrap();
    let receipt_id = current_plan_receipt_id("plan-two", 2);
    let outbox = serde_json::to_value(
        backend
            .read_mutation_outbox(GRAPH, &receipt_id)
            .await
            .unwrap(),
    )
    .unwrap();
    let method = Method::PublishCurrentPlan { request: published };
    let replay: CurrentPlanPublished = decode_raw(
        &test_support::dispatch(state, signed_run_request(521, method.clone(), "plan-two")).await,
        "expired historical replay",
    );
    assert_eq!(receipt, replay);
    let read: Option<CurrentPlanSnapshot> = decode_raw(
        &test_support::dispatch(
            state,
            signed_run_request(
                522,
                Method::GetCurrentPlan {
                    tenant: TENANT.into(),
                    run_id: "run-owner-admission".into(),
                },
                "expired-plan-read",
            ),
        )
        .await,
        "expired plan read",
    );
    assert!(read.is_none());
    let denied = test_support::dispatch(
        state,
        signed_request_as(523, method, "plan-two", "agent:foreign-owner"),
    )
    .await;
    assert!(denied.error.is_some());
    assert_eq!(
        backend.read_mutation_graph_version(GRAPH).await.unwrap(),
        version
    );
    assert_eq!(
        serde_json::to_value(
            backend
                .read_mutation_outbox(GRAPH, &receipt_id)
                .await
                .unwrap()
        )
        .unwrap(),
        outbox
    );
}

// spec: EG-DURABLE-KERNEL-R028.4.1
#[tokio::test]
async fn run_admission_owner_read_survives_native_changes_and_restart() {
    use eg_types::work_item_read::RunWorkItemView;
    let (dir, concrete, state) = fresh_run_state("eg-run-admission-read").await;
    let Method::SubmitWorkItem { request: original } = run_submit_method() else {
        unreachable!()
    };
    let admitted: SubmitWorkItemResult = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(
                900,
                Method::SubmitWorkItem {
                    request: original.clone(),
                },
                "recover-admission",
            ),
        )
        .await,
        "admit recovery run",
    );
    let get = || Method::GetRunWorkItem {
        completed_text_history: false,
        selection: None,
        tenant: TENANT.into(),
        run_id: original.run_id.clone().unwrap(),
    };
    assert_original_admission_reads(&state, &original, &admitted, concrete.as_ref()).await;
    change_and_cancel_admission(&state, &original, &admitted).await;
    let (reopened, restarted) = reopen_run_state(state, concrete, &dir).await;
    let restored: Option<RunWorkItemView> = decode_raw(
        &test_support::dispatch(&restarted, signed_run_request(908, get(), "restart-read")).await,
        "read terminal run after restart",
    );
    let restored = restored.unwrap();
    assert_eq!(restored.admission, original);
    assert_eq!(
        restored.work_item.metadata,
        serde_json::json!({"changed":true})
            .as_object()
            .unwrap()
            .clone()
    );
    assert_eq!(restored.work_item.status.as_stored(), "cancelled");
    drop(restarted);
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}

async fn assert_original_admission_reads(
    state: &test_support::SharedState,
    original: &SubmitWorkItemRequest,
    admitted: &SubmitWorkItemResult,
    backend: &RedbBackend,
) {
    use eg_types::work_item_read::RunWorkItemView;
    let get = || Method::GetRunWorkItem {
        completed_text_history: false,
        selection: None,
        tenant: TENANT.into(),
        run_id: original.run_id.clone().unwrap(),
    };
    let found: Option<RunWorkItemView> = decode_raw(
        &test_support::dispatch(state, signed_run_request(901, get(), "read-admission")).await,
        "read immutable admission",
    );
    let found = found.unwrap();
    assert_eq!(&found.admission, original);
    assert_eq!(found.work_item.work_item_id, admitted.work_item_id);
    let version = backend.read_mutation_graph_version(GRAPH).await.unwrap();
    let foreign: Option<RunWorkItemView> = decode_raw(
        &test_support::dispatch(
            state,
            signed_request_as(902, get(), "foreign-read", "agent:foreign-owner"),
        )
        .await,
        "foreign owner read",
    );
    assert!(foreign.is_none());
    let absent: Option<RunWorkItemView> = decode_raw(
        &test_support::dispatch(
            state,
            signed_run_request(
                903,
                Method::GetRunWorkItem {
                    completed_text_history: false,
                    selection: None,
                    tenant: TENANT.into(),
                    run_id: "missing".into(),
                },
                "missing-read",
            ),
        )
        .await,
        "missing run read",
    );
    assert_eq!(foreign, absent);
    let mismatch = test_support::dispatch(
        state,
        signed_run_request(
            904,
            Method::GetRunWorkItem {
                completed_text_history: false,
                selection: None,
                tenant: "foreign-tenant".into(),
                run_id: original.run_id.clone().unwrap(),
            },
            "tenant-read",
        ),
    )
    .await;
    assert!(mismatch.error.as_deref().unwrap().contains("ACCESS_DENIED"));
    // The ledger still refuses refreshed context under the old mutation key.
    let mut refreshed = original.clone();
    refreshed.context.issued_at_ms += 1;
    refreshed.context.expires_at_ms += 1;
    let conflict = test_support::dispatch(
        state,
        signed_run_request(
            905,
            Method::SubmitWorkItem { request: refreshed },
            "recover-admission",
        ),
    )
    .await;
    assert!(conflict
        .error
        .as_deref()
        .unwrap()
        .contains("IDEMPOTENCY_CONFLICT"));
    assert_eq!(
        backend.read_mutation_graph_version(GRAPH).await.unwrap(),
        version
    );
}

async fn change_and_cancel_admission(
    state: &test_support::SharedState,
    original: &SubmitWorkItemRequest,
    admitted: &SubmitWorkItemResult,
) {
    use eg_types::epistemic_operations_ext::{
        CasWorkItemMetadataRequest, CasWorkItemMetadataRequestSchemaVersion,
    };
    let changed = test_support::dispatch(
        state,
        signed_run_request(
            906,
            Method::CasWorkItemMetadata {
                request: CasWorkItemMetadataRequest {
                    schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                    tenant_ref: TENANT.into(),
                    work_item_id: admitted.work_item_id.clone(),
                    expected_lease: None,
                    expected_status: vec!["ready".into()],
                    expected_checkpoint_id: None,
                    set_checkpoint_id: None,
                    expected_metadata_msgpack: Some(
                        rmp_serde::to_vec_named(&original.metadata).unwrap(),
                    ),
                    set_metadata_msgpack: Some(
                        rmp_serde::to_vec_named(&serde_json::json!({"changed":true})).unwrap(),
                    ),
                    expected_prio_bucket: None,
                    set_prio_bucket: None,
                    now_ms: 0,
                },
            },
            "change-metadata",
        ),
    )
    .await;
    assert!(changed.error.is_none(), "metadata CAS: {changed:?}");
    let cancelled = test_support::dispatch(
        state,
        signed_run_request(
            907,
            Method::CancelWorkItem {
                tenant: TENANT.into(),
                work_item_id: admitted.work_item_id.clone(),
                idempotency_key: "cancel-recovery-run".into(),
                reason_ref: None,
                now_ms: 0,
            },
            "cancel-recovery-run",
        ),
    )
    .await;
    assert!(cancelled.error.is_none(), "cancel: {cancelled:?}");
}

// spec: EG-DURABLE-KERNEL-R028.4.1
#[tokio::test]
async fn run_admission_concurrent_retry_recovers_exact_winner() {
    use eg_types::work_item_read::RunWorkItemView;
    let (dir, concrete, state) = fresh_run_state("eg-run-admission-race").await;
    let Method::SubmitWorkItem { request: original } = run_submit_method() else {
        unreachable!()
    };
    let mut refreshed = original.clone();
    refreshed.context.issued_at_ms += 1;
    refreshed.context.expires_at_ms += 1;
    let a = signed_run_request(
        920,
        Method::SubmitWorkItem {
            request: original.clone(),
        },
        "racing-admission",
    );
    let b = signed_run_request(
        921,
        Method::SubmitWorkItem {
            request: refreshed.clone(),
        },
        "racing-admission",
    );
    let (a, b) = tokio::join!(
        test_support::dispatch(&state, a),
        test_support::dispatch(&state, b)
    );
    let (winner, loser, expected) = if a.error.is_none() {
        (&a, &b, original)
    } else {
        (&b, &a, refreshed)
    };
    let admitted: SubmitWorkItemResult = decode_raw(winner, "race winner");
    assert!(loser
        .error
        .as_deref()
        .unwrap()
        .contains("IDEMPOTENCY_CONFLICT"));
    // A lost submit response needs only an owner read, never a rewritten historical request.
    let recovered: Option<RunWorkItemView> = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(
                922,
                Method::GetRunWorkItem {
                    completed_text_history: false,
                    selection: None,
                    tenant: TENANT.into(),
                    run_id: expected.run_id.clone().unwrap(),
                },
                "race-recovery",
            ),
        )
        .await,
        "race recovery",
    );
    let recovered = recovered.unwrap();
    assert_eq!(recovered.admission, expected);
    assert_eq!(recovered.work_item.work_item_id, admitted.work_item_id);
    drop(state);
    concrete.shutdown();
    drop(concrete);
    let _ = std::fs::remove_dir_all(dir);
}

fn admission_receipt_id() -> String {
    work_item_receipt_id(&[
        GRAPH,
        TENANT,
        "work-item-inner-key",
        "admit",
        "admission",
        "run-owner-admission",
    ])
}

async fn admission_durable_bytes(backend: &RedbBackend, item: &str) -> Vec<u8> {
    let receipt = admission_receipt_id();
    let record = backend.read_mutation_batch(GRAPH, &receipt).await.unwrap();
    assert!(
        record.is_some(),
        "the canonical admission receipt must exist"
    );
    let version = backend.read_mutation_graph_version(GRAPH).await.unwrap();
    let row = backend.read_node(GRAPH, item).await.unwrap();
    assert!(
        version.is_some_and(|value| value > 0),
        "admission must advance the durable graph version"
    );
    assert!(row.is_some(), "the admitted WorkItem row must exist");
    serde_json::to_vec(&(
        record,
        backend.read_mutation_outbox(GRAPH, &receipt).await.unwrap(),
        version,
        row,
    ))
    .unwrap()
}

fn altered_admission(case: &str) -> Method {
    let mut method = run_submit_method();
    let Method::SubmitWorkItem { request } = &mut method else {
        unreachable!()
    };
    match case {
        "policy" => request.policy_digest = "e".repeat(64),
        "input" => request.input_ref = "changed-input".into(),
        "command" => request.command_digest = "f".repeat(64),
        "request" => request.context.request_id = "fresh-request".into(),
        "trace" => request.context.trace_id = "fresh-trace".into(),
        "issued" => request.context.issued_at_ms += 1,
        "expires" => request.context.expires_at_ms += 1,
        "placement" => request.context.placement_epoch = Some(1),
        _ => panic!("unknown admission mutation"),
    }
    method
}

async fn assert_strict_admission_recovery(
    state: &test_support::SharedState,
    backend: &RedbBackend,
    admitted: &SubmitWorkItemResult,
    first_id: u64,
    original_bytes: &[u8],
) {
    let replay: SubmitWorkItemResult = decode_raw(
        &test_support::dispatch(
            state,
            signed_run_request(first_id, run_submit_method(), "strict-admission"),
        )
        .await,
        "fresh outer nonce preserves exact submission replay",
    );
    assert!(replay.replayed);
    assert!(!replay.created);
    assert_eq!(replay.work_item_id, admitted.work_item_id);
    assert_eq!(
        admission_durable_bytes(backend, &admitted.work_item_id).await,
        original_bytes
    );
    for (index, case) in [
        "policy",
        "input",
        "command",
        "request",
        "trace",
        "issued",
        "expires",
        "placement",
    ]
    .into_iter()
    .enumerate()
    {
        let response = test_support::dispatch(
            state,
            signed_run_request(
                first_id + 1 + index as u64,
                altered_admission(case),
                "strict-admission",
            ),
        )
        .await;
        assert!(
            response
                .error
                .as_deref()
                .unwrap_or("")
                .contains("IDEMPOTENCY_CONFLICT"),
            "{case}: {response:?}"
        );
        assert_eq!(
            admission_durable_bytes(backend, &admitted.work_item_id).await,
            original_bytes,
            "{case} must not mutate receipt, outbox, version or row"
        );
    }
    let found: Option<eg_types::work_item_read::RunWorkItemView> = decode_raw(
        &test_support::dispatch(
            state,
            signed_run_request(
                first_id + 20,
                Method::GetRunWorkItem {
                    completed_text_history: false,
                    selection: None,
                    tenant: TENANT.into(),
                    run_id: "run-owner-admission".into(),
                },
                "strict-owner-read",
            ),
        )
        .await,
        "owner recovery after refused changed submissions",
    );
    let Method::SubmitWorkItem { request: original } = run_submit_method() else {
        unreachable!()
    };
    let found = found.unwrap();
    assert_eq!(found.admission, original);
    assert_eq!(found.work_item.work_item_id, admitted.work_item_id);
    assert_eq!(
        admission_durable_bytes(backend, &admitted.work_item_id).await,
        original_bytes
    );
}

// spec: EG-DURABLE-KERNEL-R028.4.1
#[tokio::test]
async fn strict_run_admission_refusals_preserve_durable_evidence_after_restart() {
    let (dir, concrete, state) = fresh_run_state("eg-strict-run-admission").await;
    let admitted: SubmitWorkItemResult = decode_raw(
        &test_support::dispatch(
            &state,
            signed_run_request(1000, run_submit_method(), "strict-admission"),
        )
        .await,
        "original strict admission",
    );
    let original_bytes = admission_durable_bytes(concrete.as_ref(), &admitted.work_item_id).await;
    assert_strict_admission_recovery(&state, concrete.as_ref(), &admitted, 1001, &original_bytes)
        .await;
    let (reopened, restarted) = reopen_run_state(state, concrete, &dir).await;
    assert_strict_admission_recovery(
        &restarted,
        reopened.as_ref(),
        &admitted,
        1100,
        &original_bytes,
    )
    .await;
    drop(restarted);
    reopened.shutdown();
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

// spec: EG-DURABLE-KERNEL-R028.4.3
#[tokio::test]
async fn terminal_history_claim_replay_never_reissues_fresh_execution_grant() {
    use eg_types::inline_history::NativeClaimWorkItemResult as ClaimWorkItemResult;
    let (dir, concrete, state) = fresh_run_state("eg-history-claim-freshness").await;
    let mut submit = run_submit_method();
    if let Method::SubmitWorkItem { request } = &mut submit {
        request.conversation = Some(Box::new(serde_json::from_value(serde_json::json!({
            "schema_version":"1", "context_id":"freshness-context", "expected_revision":0,
            "predecessor_run_id":null, "history_profile":{"version":1,"user_text_digest":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}
        })).unwrap()));
    }
    let response = test_support::dispatch(
        &state,
        signed_run_request(801, submit, "history-freshness-submit"),
    )
    .await;
    let submitted: SubmitWorkItemResult = decode_raw(&response, "profile admission");
    let mut claim = claim_method(WORKER);
    if let Method::ClaimWorkItem {
        request,
        completed_text_history,
    } = &mut claim
    {
        *completed_text_history = true;
        request.work_item_id = Some(submitted.work_item_id.clone());
        request.lease_ms = 60_000;
    }
    let before = concrete
        .read_node(GRAPH, &submitted.work_item_id)
        .await
        .unwrap()
        .unwrap();
    let mut opt_out = claim.clone();
    if let Method::ClaimWorkItem {
        completed_text_history,
        ..
    } = &mut opt_out
    {
        *completed_text_history = false;
    }
    let refused = test_support::dispatch(
        &state,
        signed_run_request(807, opt_out, "history-optout-claim"),
    )
    .await;
    assert!(
        refused.error.is_some(),
        "legacy claim must refuse profile before effects"
    );
    assert_eq!(
        concrete
            .read_node(GRAPH, &submitted.work_item_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    let foreign = test_support::dispatch(
        &state,
        signed_request_with_agent(
            806,
            claim.clone(),
            "history-foreign-claim",
            "agent:foreign-owner",
            "agent:delegated-run-worker",
        ),
    )
    .await;
    assert!(
        foreign.error.is_some(),
        "foreign owner must not obtain profile grant"
    );
    assert_eq!(
        concrete
            .read_node(GRAPH, &submitted.work_item_id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    let response = test_support::dispatch(
        &state,
        signed_run_request(802, claim.clone(), "history-freshness-claim"),
    )
    .await;
    let first: ClaimWorkItemResult = decode_raw(&response, "fresh profile claim");
    assert!(first.claimed);
    assert_eq!(first.attempt, Some(1));
    assert_eq!(first.fresh_execution_grant, Some(true));

    // Simulate an uncertain/lost reply by retrying with a new transport nonce.
    let response = test_support::dispatch(
        &state,
        signed_run_request(803, claim.clone(), "history-freshness-claim"),
    )
    .await;
    let replay: ClaimWorkItemResult = decode_raw(&response, "profile claim replay");
    assert_eq!(replay.lease_epoch, first.lease_epoch);
    assert_eq!(replay.fencing_token, first.fencing_token);
    assert_eq!(replay.work_item_id, first.work_item_id);
    assert_eq!(replay.fresh_execution_grant, Some(false));

    let response = test_support::dispatch(
        &state,
        signed_run_request(804, claim.clone(), "history-freshness-another-claim"),
    )
    .await;
    let in_flight: ClaimWorkItemResult = decode_raw(&response, "already leased profile run");
    assert!(!in_flight.claimed);
    assert_ne!(in_flight.fresh_execution_grant, Some(true));

    let (reopened, restarted) = reopen_run_state(state, concrete, &dir).await;
    let response = test_support::dispatch(
        &restarted,
        signed_run_request(805, claim, "history-freshness-claim"),
    )
    .await;
    let replay: ClaimWorkItemResult = decode_raw(&response, "reopened profile claim replay");
    assert_eq!(replay.lease_epoch, first.lease_epoch);
    assert_eq!(replay.fencing_token, first.fencing_token);
    assert_eq!(replay.fresh_execution_grant, Some(false));
    drop(restarted);
    reopened.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}
