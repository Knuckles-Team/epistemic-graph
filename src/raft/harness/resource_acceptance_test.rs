//! RMDD-27 public native-resource cluster acceptance evidence.
//!
//! This test deliberately drives the public signed dispatch boundary, rather than
//! calling a sealed-command helper directly.  The assertions cover the result
//! returned by the native state-machine apply, follower redirects for both writes
//! and authority reads, a ReadIndex-backed status read, leader failover, and
//! catch-up of the killed leader after restart, plus a public WorkItem claim and
//! reservation lifecycle.  A separate bounded scenario races two genuinely
//! concurrent signed public reservation calls for one WorkItem attempt and proves
//! one durable winner, one conflict, and one held charge.  Direct backend polls
//! used during follower/restart catch-up are harness synchronization only; public
//! exact-query/status dispatch is the externally visible evidence.  The direct-redb
//! last-slot race remains a separate lower-level proof.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Barrier, RwLock};

use super::super::test_env::configure_auth_test_environment;
use super::{Cluster, GRAPH};
use crate::acl::RequestContextClaims;
use crate::epistemic_operations::{
    ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ClaimWorkItemResult,
    ClaimWorkItemResultReason, OperationResult, OperationResultStatus, ResourceCapacity,
    ResourceHostUpdateRequest, ResourceHostUpdateRequestSchemaVersion,
    ResourceHostUpdateRequestTargetKind, ResourceHostUpdateResult, ResourceHostUpdateResultReason,
    ResourceRequirement, ResourceReservationRequest, ResourceReservationRequestSchemaVersion,
    ResourceReservationRequestTargetKind, ResourceReservationResult,
    ResourceReservationResultDecision, ResourceReservationResultState,
    ResourceReservationStatusRequest, ResourceReservationStatusRequestSchemaVersion,
    ResourceReservationStatusResult, ResourceReservationSummaryState,
};
use crate::isolation::{AgentIdentity, AgentRole};
use crate::protocol::{GraphType, Method, Request, ResultPayload};
use crate::raft::NodeId;
use crate::server::persistence::PersistenceBackend;
use crate::server::{compute_verified_envelope_token, dispatch, VerifiedEnvelopeParams};

const SECRET: &str = "harness";
// `cfg(test)` request-context verification deliberately pins the shared harness
// tenant/policy.  Keep the resource rows unique by graph/IDs while honoring that
// public auth contract.
const TENANT: &str = "tenant-shared";
const HOST: &str = "rmdd27-resource-host";
const AUTH_AGENT: &str = "rmdd27-resource-public-test";
const CONFLICT_AGENT: &str = "rmdd27-resource-conflict-test";
const RACE_AGENT_A: &str = "rmdd27-resource-race-agent-a";
const RACE_AGENT_B: &str = "rmdd27-resource-race-agent-b";
const WORK_ITEM: &str = "rmdd27-resource-work-item";
const WORKER: &str = "rmdd27-resource-worker";
const REPOSITORY: &str = "rmdd27-resource-repository";
const JOB: &str = "rmdd27-resource-job";
const PROFILE: &str = "rmdd27-resource-profile";
const CONCURRENCY_KEY: &str = "rmdd27-resource-concurrency";
const DISK_POLICY: &str = "rmdd27-resource-disk-policy";
const RESERVATION: &str = "rmdd27-resource-reservation";
const CONFLICT_RESERVATION: &str = "rmdd27-resource-conflict-reservation";
const RACE_RESERVATION_A: &str = "rmdd27-resource-race-reservation-a";
const RACE_RESERVATION_B: &str = "rmdd27-resource-race-reservation-b";
const IMMUTABLE_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

static REQUEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Own the cluster for the duration of the acceptance test.  The happy path
/// awaits the full teardown; panic/unwind paths cannot await, so `Drop` invokes
/// the bounded harness-only synchronous abort to stop listeners, release state
/// handles, heal partitions, and remove the temporary root on a best-effort basis.
struct ClusterGuard {
    cluster: Option<Cluster>,
}

impl ClusterGuard {
    fn new(cluster: Cluster) -> Self {
        Self {
            cluster: Some(cluster),
        }
    }

    async fn finish(mut self) {
        if let Some(cluster) = self.cluster.take() {
            cluster.teardown().await;
        }
    }
}

impl Deref for ClusterGuard {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        self.cluster
            .as_ref()
            .expect("cluster guard remains armed until teardown")
    }
}

impl DerefMut for ClusterGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.cluster
            .as_mut()
            .expect("cluster guard remains armed until teardown")
    }
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        if let Some(cluster) = self.cluster.as_mut() {
            cluster.abort_sync();
        }
        self.cluster = None;
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_millis() as u64
}

fn host_method(revision: u64, now_ms: u64) -> Method {
    Method::UpdateResourceHost {
        request: ResourceHostUpdateRequest {
            schema_version: ResourceHostUpdateRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            host_ref: HOST.to_string(),
            revision,
            capacity: ResourceCapacity {
                cpu_weight: 128,
                memory_mib: 16_384,
                disk_mib: 100_000,
                process_slots: 32,
            },
            observed: ResourceCapacity {
                // Admission adds observed + held + requested dimensions. Keep
                // live usage bounded so the public reservation can be accepted;
                // disk_used_mib below remains an independent filesystem value.
                cpu_weight: 0,
                memory_mib: 0,
                disk_mib: 0,
                process_slots: 0,
            },
            heartbeat_at_ms: now_ms,
            heartbeat_ttl_ms: 60_000,
            now_ms,
            draining: false,
            quarantined: false,
            labels: vec!["linux".to_string(), "rmdd27".to_string()],
            target_kind: ResourceHostUpdateRequestTargetKind::Local,
            target_alias: None,
            disk_used_mib: 100,
            disk_capacity_mib: 100_000,
        },
    }
}

fn status_method(now_ms: u64) -> Method {
    Method::ResourceReservationStatus {
        request: ResourceReservationStatusRequest {
            schema_version: ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            work_item_id: None,
            reservation_id: None,
            host_ref: Some(HOST.to_string()),
            owner_id: None,
            fence: None,
            attempt: None,
            lease_epoch: None,
            fencing_token: None,
            input_fingerprint: None,
            fairness_group: Some("default".to_string()),
            limit: 32,
            cursor: None,
            now_ms,
        },
    }
}

fn resource_b64_urlsafe(value: &str) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let bytes = value.as_bytes();
    let mut encoded = String::with_capacity(((bytes.len() + 2) / 3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        if chunk.len() == 1 {
            encoded.push(ALPHABET[((first & 0x03) << 4) as usize] as char);
            encoded.push('=');
            encoded.push('=');
            continue;
        }
        let second = chunk[1];
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() == 2 {
            encoded.push(ALPHABET[((second & 0x0f) << 2) as usize] as char);
            encoded.push('=');
            continue;
        }
        let third = chunk[2];
        encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
    }
    let chunks: Vec<String> = encoded
        .as_bytes()
        .chunks(3)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect();
    format!("opaque:v1:{}", chunks.join("."))
}

fn work_item_method(now_ms: u64) -> Method {
    let immutable_digest = IMMUTABLE_DIGEST;
    let props = serde_json::json!({
        "node_type": "WorkItem",
        "tenant": TENANT,
        "status": "ready",
        "lease_owner": null,
        "last_lease_owner": null,
        "attempt": 0,
        "lease_epoch": 0,
        "fencing_token": 0,
        "lease_expires_at": 0.0,
        "max_attempts": 3,
        "created_at": now_ms as f64 / 1000.0,
        "updated_at": now_ms as f64 / 1000.0,
        "heartbeat_at": now_ms as f64 / 1000.0,
        "next_retry_at": 0.0,
        "prio_bucket": 0,
        "kind": "repository_work_item",
        "payload_ref": "rmdd27-resource-payload",
        "resource_class": PROFILE,
        "fairness_group": "default",
        "metadata": {
            "repository_work_item": {
                "contract_version": "1",
                "immutable_input_digest": immutable_digest,
                "tenant_id": resource_b64_urlsafe(TENANT),
                "repository_id": resource_b64_urlsafe(REPOSITORY),
                "owner_id": resource_b64_urlsafe(WORKER),
                "branch": resource_b64_urlsafe("main"),
                "job_id": resource_b64_urlsafe(JOB),
                "target_kind": "local",
                "target_alias": null,
                "priority": 0,
                "queue_deadline": null,
                "resource_reservation": {
                    "schema_version": "1",
                    "resolved_profile_authority": "repository_manager:resource_profile_registry:v1",
                    "profile_name": resource_b64_urlsafe(PROFILE),
                    "profile_version": resource_b64_urlsafe("1"),
                    "cpu_weight": 2,
                    "memory_mib": 1024,
                    "disk_mib": 200,
                    "process_slots": 1,
                    "host_labels": [resource_b64_urlsafe("linux")],
                    "anti_affinity": [resource_b64_urlsafe("compiler")],
                    "preferred_target": {
                        "contract_version": "1",
                        "kind": "local",
                        "alias": null,
                        "capability_labels": [],
                    },
                    "required_target": null,
                    "repository_id": resource_b64_urlsafe(REPOSITORY),
                    "concurrency_key": resource_b64_urlsafe(CONCURRENCY_KEY),
                    "concurrency_limit": 1,
                    "repository_exclusive": true,
                    "branch_exclusive": true,
                    "fairness_group": resource_b64_urlsafe("default"),
                    "fairness_cost": 1,
                    "disk_policy_key": resource_b64_urlsafe(DISK_POLICY),
                    "disk_low_watermark_mib": 500,
                    "disk_high_watermark_mib": 800,
                    "branch": resource_b64_urlsafe("main"),
                    "branch_explicit": true,
                    "base_ref": resource_b64_urlsafe("main"),
                    "target_kind": "local",
                    "target_alias": null,
                    "work_item_input_fingerprint": format!("v1:{immutable_digest}"),
                },
            }
        }
    });
    Method::AddNode {
        node_id: WORK_ITEM.to_string(),
        properties_msgpack: rmp_serde::to_vec_named(&props).expect("encode WorkItem properties"),
    }
}

fn reservation_request(
    now_ms: u64,
    expected_lifecycle_revision: Option<u64>,
) -> ResourceReservationRequest {
    let mut request = ResourceReservationRequest {
        schema_version: ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: TENANT.to_string(),
        work_item_id: WORK_ITEM.to_string(),
        owner_id: WORKER.to_string(),
        fence: "1".to_string(),
        lease_epoch: 1,
        fencing_token: 1,
        attempt: 1,
        reservation_id: RESERVATION.to_string(),
        input_fingerprint: String::new(),
        profile_name: PROFILE.to_string(),
        profile_version: "1".to_string(),
        host_ref: HOST.to_string(),
        requirement: ResourceRequirement {
            cpu_weight: 2,
            memory_mib: 1024,
            disk_mib: 200,
            process_slots: 1,
        },
        target_kind: ResourceReservationRequestTargetKind::Local,
        target_alias: None,
        repository_id: REPOSITORY.to_string(),
        branch: "main".to_string(),
        concurrency_key: CONCURRENCY_KEY.to_string(),
        concurrency_limit: Some(1),
        repository_exclusive: true,
        branch_exclusive: true,
        required_labels: vec!["linux".to_string()],
        anti_affinity: vec!["compiler".to_string()],
        fairness_group: "default".to_string(),
        fairness_cost: 1,
        disk_low_watermark_mib: Some(500),
        disk_high_watermark_mib: Some(800),
        disk_policy_key: DISK_POLICY.to_string(),
        reserved_at_ms: now_ms,
        expires_at_ms: now_ms.saturating_add(60_000),
        idempotency_key: "rmdd27-resource-reservation-idempotency".to_string(),
        now_ms,
        expected_host_revision: Some(1),
        expected_lifecycle_revision,
    };
    request.input_fingerprint = reservation_input_fingerprint(&request);
    request
}

fn reservation_input_fingerprint(request: &ResourceReservationRequest) -> String {
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;

    let preferred_target = serde_json::json!({
        "alias": null,
        "capability_labels": [],
        "contract_version": "1",
        "kind": "local",
    });
    let mut resources = BTreeMap::new();
    resources.insert("anti_affinity", serde_json::json!(request.anti_affinity));
    resources.insert(
        "concurrency_key",
        serde_json::json!(request.concurrency_key),
    );
    resources.insert("contract_version", serde_json::json!("1"));
    resources.insert(
        "cpu_weight",
        serde_json::json!(request.requirement.cpu_weight),
    );
    resources.insert(
        "disk_high_watermark_mib",
        request
            .disk_high_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    resources.insert(
        "disk_low_watermark_mib",
        request
            .disk_low_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    resources.insert("disk_mib", serde_json::json!(request.requirement.disk_mib));
    resources.insert("fairness_group", serde_json::json!(request.fairness_group));
    resources.insert("host_labels", serde_json::json!(request.required_labels));
    resources.insert(
        "memory_mib",
        serde_json::json!(request.requirement.memory_mib),
    );
    resources.insert("preferred_target", preferred_target);
    resources.insert("priority", serde_json::json!(0));
    resources.insert(
        "process_slots",
        serde_json::json!(request.requirement.process_slots),
    );
    resources.insert("queue_deadline", serde_json::Value::Null);
    resources.insert("required_target", serde_json::Value::Null);
    resources.insert("resource_class", serde_json::json!(request.profile_name));

    let mut payload = BTreeMap::new();
    payload.insert("attempt", serde_json::json!(request.attempt));
    payload.insert("branch", serde_json::json!(request.branch));
    payload.insert("fence", serde_json::json!(request.fence));
    payload.insert("job_id", serde_json::json!(JOB));
    payload.insert("owner_id", serde_json::json!(request.owner_id));
    payload.insert("profile", serde_json::json!(request.profile_name));
    payload.insert("profile_version", serde_json::json!(1));
    payload.insert("repository_id", serde_json::json!(request.repository_id));
    payload.insert("reservation_id", serde_json::json!(request.reservation_id));
    payload.insert(
        "resources",
        serde_json::to_value(resources).expect("serialize resource fingerprint resources"),
    );
    payload.insert("tenant_id", serde_json::json!(request.tenant_ref));
    payload.insert(
        "ttl_seconds",
        serde_json::json!(request.expires_at_ms.saturating_sub(request.reserved_at_ms) / 1_000),
    );
    payload.insert("version", serde_json::json!("v1"));
    payload.insert("work_item_id", serde_json::json!(request.work_item_id));
    let bytes = serde_json::to_vec(&payload).expect("serialize resource fingerprint payload");
    format!("v1:{}", hex::encode(Sha256::digest(bytes)))
}

fn reservation_query_method(request: &ResourceReservationRequest, now_ms: u64) -> Method {
    Method::QueryWorkItemReservation {
        request: ResourceReservationStatusRequest {
            schema_version: ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: request.tenant_ref.clone(),
            work_item_id: Some(request.work_item_id.clone()),
            reservation_id: Some(request.reservation_id.clone()),
            host_ref: Some(request.host_ref.clone()),
            owner_id: Some(request.owner_id.clone()),
            fence: Some(request.fence.clone()),
            attempt: Some(request.attempt),
            lease_epoch: Some(request.lease_epoch),
            fencing_token: Some(request.fencing_token),
            input_fingerprint: Some(request.input_fingerprint.clone()),
            fairness_group: Some(request.fairness_group.clone()),
            limit: 1,
            cursor: None,
            now_ms,
        },
    }
}

fn signed_request(request_id: u64, method: Method) -> Request {
    signed_request_as(request_id, AUTH_AGENT, method)
}

fn signed_request_as(request_id: u64, agent_id: &str, method: Method) -> Request {
    let context = RequestContextClaims {
        principal: agent_id.to_string(),
        tenant: TENANT.to_string(),
        audience: "epistemic-graph-test".to_string(),
        agent_id: agent_id.to_string(),
        roles: vec!["resource-controller".to_string()],
        // Wildcard includes both the controller write scope and the native
        // reservation read scope; the test is specifically about cluster routing,
        // not the separate capability-ledger unit tests.
        scopes: vec!["*".to_string()],
        policy_version: "policy-test".to_string(),
        delegation: Vec::new(),
        node: None,
        priority: None,
    };
    let mut request = Request {
        id: request_id,
        graph: GRAPH.to_string(),
        auth_token: String::new(),
        agent_id: Some(agent_id.to_string()),
        method,
    };
    let sequence = REQUEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch");
    let nonce = format!(
        "rmdd27-resource-{}-{request_id}-{sequence}-{}",
        std::process::id(),
        issued_at.as_nanos()
    );
    let idempotency_key = format!("rmdd27-resource-request-{request_id}-{sequence}");
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp: issued_at.as_secs(),
            nonce: &nonce,
            idempotency_key: &idempotency_key,
        },
    );
    request
}

async fn attach_multi_raft(cluster: &Cluster) {
    let nodes: Vec<_> = cluster
        .members
        .values()
        .filter_map(|member| {
            Some((
                member.state.as_ref()?.clone(),
                member.started.as_ref()?.multi.clone(),
            ))
        })
        .collect();
    for (state, multi) in nodes {
        state.write().await.multi_raft = Some(multi);
    }
}

async fn ensure_commons_graph(cluster: &Cluster) {
    let nodes: Vec<_> = cluster
        .members
        .values()
        .filter_map(|member| member.state.as_ref().cloned())
        .collect();
    for state in nodes {
        let backend = state
            .read()
            .await
            .persistence
            .clone()
            .expect("cluster member has persistence");
        backend
            .register_graph(&crate::persist::sanitize(GRAPH), GRAPH, GraphType::Commons)
            .await
            .expect("persist commons graph identity");
        let mut server = state.write().await;
        // The public graph mutation boundary requires provisioned identities;
        // System is the harness-only role used by these signed public clients.
        for agent_id in [AUTH_AGENT, CONFLICT_AGENT, RACE_AGENT_A, RACE_AGENT_B] {
            server.isolation.register_agent(AgentIdentity {
                agent_id: agent_id.to_string(),
                role: AgentRole::System,
                teams: Vec::new(),
                roles: Vec::new(),
            });
        }
        if !server.registry.exists(GRAPH) {
            server
                .registry
                .create_graph(GRAPH, GraphType::Commons, None)
                .expect("register commons graph in the test registry");
        }
    }
}

fn state_for(cluster: &Cluster, node_id: NodeId) -> Arc<RwLock<crate::server::ServerState>> {
    cluster
        .members
        .get(&node_id)
        .and_then(|member| member.state.clone())
        .expect("requested cluster member is running")
}

async fn dispatch_host_update(
    cluster: &Cluster,
    node_id: NodeId,
    request_id: u64,
    revision: u64,
) -> crate::protocol::Response {
    let now_ms = unix_ms();
    dispatch(
        &state_for(cluster, node_id),
        signed_request(request_id, host_method(revision, now_ms)),
    )
    .await
}

async fn dispatch_status(
    cluster: &Cluster,
    node_id: NodeId,
    request_id: u64,
) -> crate::protocol::Response {
    dispatch_method(cluster, node_id, request_id, status_method(unix_ms())).await
}

async fn dispatch_method(
    cluster: &Cluster,
    node_id: NodeId,
    request_id: u64,
    method: Method,
) -> crate::protocol::Response {
    dispatch_method_as(cluster, node_id, request_id, AUTH_AGENT, method).await
}

async fn dispatch_method_as(
    cluster: &Cluster,
    node_id: NodeId,
    request_id: u64,
    agent_id: &str,
    method: Method,
) -> crate::protocol::Response {
    dispatch(
        &state_for(cluster, node_id),
        signed_request_as(request_id, agent_id, method),
    )
    .await
}

fn reservation_variant(
    base: &ResourceReservationRequest,
    reservation_id: &str,
    idempotency_key: &str,
) -> ResourceReservationRequest {
    let mut request = base.clone();
    request.reservation_id = reservation_id.to_string();
    request.idempotency_key = idempotency_key.to_string();
    request.input_fingerprint = reservation_input_fingerprint(&request);
    request
}

/// Start two independent signed public calls at the same scheduling barrier.
/// Both calls target the same current MultiRaft leader, so neither branch is a
/// follower-redirect shortcut; the serialized native command is the only race
/// arbiter.  Distinct reservation identities make the losing result a conflict
/// rather than an exact-request idempotent replay.
async fn race_public_reservation_calls(
    cluster: &Cluster,
    leader: NodeId,
    first_agent: &str,
    first_request_id: u64,
    first_method: Method,
    second_agent: &str,
    second_request_id: u64,
    second_method: Method,
) -> (crate::protocol::Response, crate::protocol::Response) {
    let barrier = Arc::new(Barrier::new(3));
    let first_state = state_for(cluster, leader);
    let second_state = state_for(cluster, leader);
    let first_request = signed_request_as(first_request_id, first_agent, first_method);
    let second_request = signed_request_as(second_request_id, second_agent, second_method);

    let first_barrier = barrier.clone();
    let first_task = tokio::spawn(async move {
        first_barrier.wait().await;
        dispatch(&first_state, first_request).await
    });
    let second_barrier = barrier.clone();
    let second_task = tokio::spawn(async move {
        second_barrier.wait().await;
        dispatch(&second_state, second_request).await
    });

    // Release both public clients only after both tasks are waiting.  Joining
    // both handles together keeps the assertions below about their two returned
    // public responses, not about a serial test-side loop.
    barrier.wait().await;
    let (first, second) = tokio::join!(first_task, second_task);
    (
        first.expect("first public reservation client task completed"),
        second.expect("second public reservation client task completed"),
    )
}

async fn wait_for_node_leader(cluster: &Cluster, node_id: NodeId, expected_leader: NodeId) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let multi = cluster
            .members
            .get(&node_id)
            .and_then(|member| member.started.as_ref())
            .map(|started| started.multi.clone())
            .expect("node is running while waiting for its leader view");
        if let Some(group) = multi.group_for_graph(GRAPH).await {
            if group.current_leader().await == Some(expected_leader) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node {node_id} did not learn leader {expected_leader}");
}

fn decode_host_result(response: crate::protocol::Response, expected_revision: u64) {
    assert_eq!(response.error, None, "native update returned an error");
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("native update did not return a typed raw result: {response:?}");
    };
    let result: ResourceHostUpdateResult = eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode ResourceHostUpdateResult");
    assert!(
        result.accepted,
        "native host update was rejected: {result:?}"
    );
    assert_eq!(result.reason, ResourceHostUpdateResultReason::Accepted);
    assert_eq!(result.host_ref, HOST);
    assert_eq!(result.revision, expected_revision);
    assert_eq!(
        result
            .host_snapshot
            .as_ref()
            .map(|snapshot| snapshot.revision),
        Some(expected_revision)
    );
}

fn decode_raw<T: serde::de::DeserializeOwned>(
    response: crate::protocol::Response,
    label: &str,
) -> T {
    assert_eq!(
        response.error, None,
        "{label} returned an error: {response:?}"
    );
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("{label} did not return a typed raw result: {response:?}");
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .unwrap_or_else(|error| panic!("decode {label} result: {error}"))
}

fn decode_claim_result(response: crate::protocol::Response) -> ClaimWorkItemResult {
    let result: ClaimWorkItemResult = decode_raw(response, "ClaimWorkItem");
    assert!(
        result.claimed,
        "public ClaimWorkItem did not claim: {result:?}"
    );
    assert_eq!(result.reason, ClaimWorkItemResultReason::Claimed);
    assert_eq!(result.work_item_id.as_deref(), Some(WORK_ITEM));
    assert_eq!(result.lease_holder_ref.as_deref(), Some(WORKER));
    assert_eq!(result.lease_epoch, Some(1));
    assert_eq!(result.fencing_token, Some(1));
    assert_eq!(result.attempt, Some(1));
    result
}

fn decode_reservation_result(
    response: crate::protocol::Response,
    label: &str,
    expected_decision: ResourceReservationResultDecision,
    expected_state: ResourceReservationResultState,
) -> ResourceReservationResult {
    decode_reservation_result_for(
        response,
        label,
        expected_decision,
        expected_state,
        RESERVATION,
    )
}

fn decode_reservation_result_for(
    response: crate::protocol::Response,
    label: &str,
    expected_decision: ResourceReservationResultDecision,
    expected_state: ResourceReservationResultState,
    expected_reservation_id: &str,
) -> ResourceReservationResult {
    let result: ResourceReservationResult = decode_raw(response, label);
    assert_eq!(result.decision, expected_decision, "{label}: {result:?}");
    assert_eq!(result.work_item_id, WORK_ITEM);
    assert_eq!(result.state, expected_state);
    assert_eq!(
        result.reservation_id.as_deref(),
        Some(expected_reservation_id)
    );
    result
}

fn decode_status_result(
    response: crate::protocol::Response,
    expected_revision: u64,
) -> ResourceReservationStatusResult {
    assert_eq!(response.error, None, "native status returned an error");
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("native status did not return a typed raw result: {response:?}");
    };
    let result: ResourceReservationStatusResult = eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode ResourceReservationStatusResult");
    assert_eq!(
        result
            .host_snapshot
            .as_ref()
            .map(|snapshot| snapshot.revision),
        Some(expected_revision),
        "status read did not observe the committed host revision: {result:?}"
    );
    result
}

fn assert_active_status(status: &ResourceReservationStatusResult) {
    assert_active_status_for(status, RESERVATION);
}

fn assert_active_status_for(status: &ResourceReservationStatusResult, reservation_id: &str) {
    assert_eq!(status.held_cpu_weight, 2);
    assert_eq!(status.held_memory_mib, 1024);
    assert_eq!(status.held_disk_mib, 200);
    assert_eq!(status.held_process_slots, 1);
    assert_eq!(status.fairness_debt, 1);
    assert_eq!(
        status
            .reservations
            .iter()
            .filter(|summary| {
                summary.state == ResourceReservationSummaryState::Reserved && !summary.tombstone
            })
            .count(),
        1,
        "exactly one active reservation must hold the charged resources"
    );
    assert!(status.reservations.iter().any(|summary| {
        summary.reservation_id == reservation_id
            && summary.state == ResourceReservationSummaryState::Reserved
            && !summary.tombstone
            && summary.held_cpu_weight == 2
            && summary.held_memory_mib == 1024
            && summary.held_disk_mib == 200
            && summary.held_process_slots == 1
    }));
}

fn assert_released_status(status: &ResourceReservationStatusResult) {
    assert_eq!(status.held_cpu_weight, 0);
    assert_eq!(status.held_memory_mib, 0);
    assert_eq!(status.held_disk_mib, 0);
    assert_eq!(status.held_process_slots, 0);
    // Fairness debt is historical service debt and must survive release.
    assert_eq!(status.fairness_debt, 1);
    assert!(status.reservations.iter().any(|summary| {
        summary.reservation_id == RESERVATION
            && summary.state == ResourceReservationSummaryState::Released
            && summary.tombstone
            && summary.held_cpu_weight == 0
            && summary.held_memory_mib == 0
            && summary.held_disk_mib == 0
            && summary.held_process_slots == 0
    }));
}

fn assert_redirect(response: crate::protocol::Response, expected_leader: NodeId) {
    assert_eq!(response.error.as_deref(), Some("OPERATION_REDIRECTED"));
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("redirect did not carry the structured operation result: {response:?}");
    };
    let detail: OperationResult = eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode operation redirect");
    assert_eq!(detail.status, OperationResultStatus::Redirected);
    let redirect = detail.redirect.expect("redirect detail");
    assert_eq!(redirect.group, 0);
    let expected_leader_ref = format!("node:{expected_leader}");
    assert_eq!(
        redirect.leader_ref.as_deref(),
        Some(expected_leader_ref.as_str())
    );
}

/// Harness-only durable-apply synchronization.  The direct backend observation
/// is not public acceptance evidence; callers follow it with signed dispatch
/// assertions for the externally visible result or redirect.
async fn wait_for_backend_revision(cluster: &Cluster, node_id: NodeId, expected_revision: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last_error = String::new();
    while tokio::time::Instant::now() < deadline {
        let state = state_for(cluster, node_id);
        let backend = state
            .read()
            .await
            .persistence
            .clone()
            .expect("cluster member has persistence");
        match backend
            .read_resource_reservation_status(
                &crate::persist::sanitize(GRAPH),
                &match status_method(unix_ms()) {
                    Method::ResourceReservationStatus { request } => request,
                    _ => unreachable!(),
                },
            )
            .await
        {
            Ok(result)
                if result
                    .host_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.revision >= expected_revision) =>
            {
                return;
            }
            Ok(result) => last_error = format!("observed status {result:?}"),
            Err(error) => last_error = error,
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!("node {node_id} did not apply host revision {expected_revision}: {last_error}");
}

async fn wait_for_public_active_status(
    cluster: &Cluster,
    node_id: NodeId,
    expected_host_revision: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut request_id = 30;
    let mut last_error = String::new();
    while tokio::time::Instant::now() < deadline {
        let response = dispatch_status(cluster, node_id, request_id).await;
        request_id = request_id.saturating_add(1);
        if let Some(error) = response.error.as_deref() {
            last_error = error.to_string();
        } else {
            match response.result {
                Some(ResultPayload::Raw(bytes)) => {
                    match eg_types::msgpack::decode_bounded::<ResourceReservationStatusResult>(
                        &bytes,
                        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
                    ) {
                        Ok(status)
                            if status.host_snapshot.as_ref().is_some_and(|snapshot| {
                                snapshot.revision >= expected_host_revision
                            }) && status.held_cpu_weight == 2
                                && status.held_memory_mib == 1024
                                && status.held_disk_mib == 200
                                && status.held_process_slots == 1
                                && status.fairness_debt == 1
                                && status.reservations.iter().any(|summary| {
                                    summary.reservation_id == RESERVATION
                                        && summary.state
                                            == ResourceReservationSummaryState::Reserved
                                        && !summary.tombstone
                                        && summary.held_cpu_weight == 2
                                        && summary.held_memory_mib == 1024
                                        && summary.held_disk_mib == 200
                                        && summary.held_process_slots == 1
                                }) =>
                        {
                            return;
                        }
                        Ok(status) => last_error = format!("observed public status {status:?}"),
                        Err(error) => last_error = format!("decode public status: {error}"),
                    }
                }
                Some(result) => last_error = format!("public status returned {result:?}"),
                None => last_error = "public status returned no result".to_string(),
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!(
        "node {node_id} did not observe the active reservation through public status: {last_error}"
    );
}

async fn seed_public_work_item_claim(cluster: &Cluster, leader: NodeId) {
    // Seed the exact Repository WorkItem admission projection through the public
    // signed graph API.  Claim and all resource mutations then use the public
    // dispatch path; no direct redb/GraphCore mutation is involved.
    let add = dispatch_method(cluster, leader, 10, work_item_method(unix_ms())).await;
    assert_eq!(add.error, None, "public WorkItem AddNode failed: {add:?}");

    let claim = dispatch_method(
        cluster,
        leader,
        11,
        Method::ClaimWorkItem {
            request: ClaimWorkItemRequest {
                schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                tenant_ref: TENANT.to_string(),
                work_item_id: Some(WORK_ITEM.to_string()),
                queue_ref: None,
                resource_class: Some(PROFILE.to_string()),
                fairness_group: Some("default".to_string()),
                worker_ref: WORKER.to_string(),
                now_ms: unix_ms(),
                lease_ms: 60_000,
                max_tenant_in_flight: 1,
            },
        },
    )
    .await;
    let claim = decode_claim_result(claim);
    let lease_epoch = claim.lease_epoch.expect("claimed lease epoch");
    let fencing_token = claim.fencing_token.expect("claimed fencing token");
    let attempt = claim.attempt.expect("claimed attempt");
    assert_eq!((lease_epoch, fencing_token, attempt), (1, 1, 1));
}

async fn drive_public_work_item_resource_setup(
    cluster: &Cluster,
    leader: NodeId,
) -> (ResourceReservationRequest, ResourceReservationResult) {
    seed_public_work_item_claim(cluster, leader).await;

    let reserve_request = reservation_request(unix_ms(), None);
    let reserved = dispatch_method(
        cluster,
        leader,
        12,
        Method::ReserveWorkItemResources {
            request: reserve_request.clone(),
        },
    )
    .await;
    let reserved = decode_reservation_result(
        reserved,
        "ReserveWorkItemResources",
        ResourceReservationResultDecision::Accepted,
        ResourceReservationResultState::Reserved,
    );
    assert_eq!(reserved.host_ref.as_deref(), Some(HOST));
    assert_eq!(reserved.host_revision, 1);
    assert_eq!(reserved.held_cpu_weight, 2);
    assert_eq!(reserved.held_memory_mib, 1024);
    assert_eq!(reserved.held_disk_mib, 200);
    assert_eq!(reserved.held_process_slots, 1);
    assert_eq!(reserved.fairness_debt, 1);
    let record = reserved
        .record
        .as_ref()
        .expect("accepted reservation record");
    assert!(record.repository_exclusive);
    assert!(record.branch_exclusive);
    assert_eq!(record.fairness_group, "default");
    assert_eq!(record.fairness_cost, 1);

    // A public retry of the exact reservation is an idempotent result, not a
    // second host hold or a generic successful Raft acknowledgement.
    let reserve_retry = dispatch_method(
        cluster,
        leader,
        18,
        Method::ReserveWorkItemResources {
            request: reserve_request.clone(),
        },
    )
    .await;
    let reserve_retry = decode_reservation_result(
        reserve_retry,
        "ReserveWorkItemResources retry",
        ResourceReservationResultDecision::Idempotent,
        ResourceReservationResultState::Reserved,
    );
    assert_eq!(
        reserve_retry.lifecycle_revision,
        reserved.lifecycle_revision
    );
    assert_eq!(reserve_retry.held_cpu_weight, reserved.held_cpu_weight);

    // A distinct public caller using the same WorkItem/attempt but a different
    // reservation identity must lose the attempt index without replacing the
    // active hold. This is a serial conflict proof, not a concurrency claim.
    let mut conflict_request = reserve_request.clone();
    conflict_request.reservation_id = CONFLICT_RESERVATION.to_string();
    conflict_request.idempotency_key = "rmdd27-resource-conflict-idempotency".to_string();
    conflict_request.input_fingerprint = reservation_input_fingerprint(&conflict_request);
    let conflict = dispatch_method_as(
        cluster,
        leader,
        19,
        CONFLICT_AGENT,
        Method::ReserveWorkItemResources {
            request: conflict_request,
        },
    )
    .await;
    let conflict: ResourceReservationResult = decode_raw(conflict, "conflicting reservation");
    assert_eq!(
        conflict.decision,
        ResourceReservationResultDecision::Conflict,
        "same-attempt reservation must not replace the winner: {conflict:?}"
    );
    assert_eq!(
        conflict.reservation_id.as_deref(),
        Some(CONFLICT_RESERVATION)
    );
    assert_eq!(conflict.state, ResourceReservationResultState::Absent);
    assert_eq!(conflict.held_cpu_weight, 0);

    // The exact query is an authority read and therefore exercises the public
    // ReadIndex path separately from the aggregate status read below.
    let queried = dispatch_method(
        cluster,
        leader,
        13,
        reservation_query_method(&reserve_request, unix_ms()),
    )
    .await;
    let queried = decode_reservation_result(
        queried,
        "QueryWorkItemReservation",
        ResourceReservationResultDecision::Idempotent,
        ResourceReservationResultState::Reserved,
    );
    assert_eq!(queried.lifecycle_revision, reserved.lifecycle_revision);
    assert_eq!(
        queried.record.as_ref().map(|record| record.state),
        Some(crate::epistemic_operations::ResourceReservationRecordState::Reserved)
    );

    let status = decode_status_result(dispatch_status(cluster, leader, 14).await, 1);
    assert_active_status(&status);

    (reserve_request, reserved)
}

async fn drive_public_release_after_failover(
    cluster: &Cluster,
    leader: NodeId,
    reserve_request: &ResourceReservationRequest,
    reserved: &ResourceReservationResult,
    stale_follower: NodeId,
) -> ResourceReservationResult {
    // The hold was committed before the failure. Query and aggregate status on
    // the newly elected leader before releasing it, proving the native row and
    // host accounting survived the Raft leadership change.
    let queried = dispatch_method(
        cluster,
        leader,
        20,
        reservation_query_method(reserve_request, unix_ms()),
    )
    .await;
    let queried = decode_reservation_result(
        queried,
        "QueryWorkItemReservation after failover",
        ResourceReservationResultDecision::Idempotent,
        ResourceReservationResultState::Reserved,
    );
    assert_eq!(queried.lifecycle_revision, reserved.lifecycle_revision);
    assert_eq!(queried.held_cpu_weight, 2);
    assert_eq!(queried.held_memory_mib, 1024);
    assert_eq!(queried.held_disk_mib, 200);
    assert_eq!(queried.held_process_slots, 1);
    assert_eq!(queried.fairness_debt, 1);
    let record = queried.record.as_ref().expect("active failover record");
    assert!(record.repository_exclusive);
    assert!(record.branch_exclusive);

    let status = decode_status_result(dispatch_status(cluster, leader, 21).await, 1);
    assert_active_status(&status);

    let mut release_request = reserve_request.clone();
    release_request.expected_lifecycle_revision = Some(reserved.lifecycle_revision);
    release_request.now_ms = unix_ms();
    let released = dispatch_method(
        cluster,
        leader,
        22,
        Method::ReleaseWorkItemResources {
            request: release_request.clone(),
        },
    )
    .await;
    let released = decode_reservation_result(
        released,
        "ReleaseWorkItemResources after failover",
        ResourceReservationResultDecision::Accepted,
        ResourceReservationResultState::Released,
    );
    assert!(released.tombstone);
    assert_eq!(released.held_cpu_weight, 0);
    assert_eq!(released.held_memory_mib, 0);
    assert_eq!(released.held_disk_mib, 0);
    assert_eq!(released.held_process_slots, 0);
    assert_eq!(released.fairness_debt, 1);

    // Re-resolve the current leader after the release before reading the terminal
    // row.  This is the public execution-handoff evidence: the exact query must be
    // answered by the authority that currently owns the placement, while a real
    // follower must redirect the same query instead of serving a stale snapshot.
    let current_leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("current leader remains discoverable after release");
    wait_for_node_leader(cluster, current_leader, current_leader).await;
    let queried_release = dispatch_method(
        cluster,
        current_leader,
        23,
        reservation_query_method(&release_request, unix_ms()),
    )
    .await;
    let queried_release = decode_reservation_result(
        queried_release,
        "QueryWorkItemReservation after failover release",
        ResourceReservationResultDecision::Idempotent,
        ResourceReservationResultState::Released,
    );
    assert_eq!(
        queried_release.lifecycle_revision,
        released.lifecycle_revision
    );
    assert!(queried_release.tombstone);

    assert!(cluster.is_running(stale_follower));
    assert_ne!(stale_follower, current_leader);
    // The caller isolated this follower after proving it held the pre-release
    // reservation. Its public query must therefore redirect rather than serve
    // that stale local row as authority.
    wait_for_backend_active_reservation(cluster, stale_follower, 1).await;
    assert_redirect(
        dispatch_method(
            cluster,
            stale_follower,
            24,
            reservation_query_method(&release_request, unix_ms()),
        )
        .await,
        current_leader,
    );

    let status = decode_status_result(dispatch_status(cluster, current_leader, 25).await, 1);
    assert_released_status(&status);
    released
}

/// Harness-only synchronization that proves a follower has the active hold
/// before the release-isolation window begins. This is deliberately a direct
/// backend observation; the signed public query below is the authority proof.
async fn wait_for_backend_active_reservation(
    cluster: &Cluster,
    node_id: NodeId,
    expected_host_revision: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last_error = String::new();
    while tokio::time::Instant::now() < deadline {
        let state = state_for(cluster, node_id);
        let backend = state
            .read()
            .await
            .persistence
            .clone()
            .expect("cluster member has persistence");
        let request = match status_method(unix_ms()) {
            Method::ResourceReservationStatus { request } => request,
            _ => unreachable!(),
        };
        match backend
            .read_resource_reservation_status(&crate::persist::sanitize(GRAPH), &request)
            .await
        {
            Ok(result)
                if result
                    .host_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.revision >= expected_host_revision)
                    && result.held_cpu_weight == 2
                    && result.held_memory_mib == 1024
                    && result.held_disk_mib == 200
                    && result.held_process_slots == 1
                    && result.fairness_debt == 1
                    && result.reservations.iter().any(|summary| {
                        summary.reservation_id == RESERVATION
                            && summary.state == ResourceReservationSummaryState::Reserved
                            && !summary.tombstone
                            && summary.held_cpu_weight == 2
                            && summary.held_memory_mib == 1024
                            && summary.held_disk_mib == 200
                            && summary.held_process_slots == 1
                    }) =>
            {
                return;
            }
            Ok(result) => last_error = format!("observed active status {result:?}"),
            Err(error) => last_error = error,
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!(
        "node {node_id} did not observe the active reservation {expected_host_revision}: {last_error}"
    );
}

/// Harness-only restart/catch-up synchronization. Public exact queries and
/// status reads, not this direct backend poll, prove the served terminal state.
async fn wait_for_backend_reservation_state(
    cluster: &Cluster,
    node_id: NodeId,
    expected_host_revision: u64,
    expected_state: ResourceReservationSummaryState,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut last_error = String::new();
    while tokio::time::Instant::now() < deadline {
        let state = state_for(cluster, node_id);
        let backend = state
            .read()
            .await
            .persistence
            .clone()
            .expect("cluster member has persistence");
        let request = match status_method(unix_ms()) {
            Method::ResourceReservationStatus { request } => request,
            _ => unreachable!(),
        };
        match backend
            .read_resource_reservation_status(&crate::persist::sanitize(GRAPH), &request)
            .await
        {
            Ok(result)
                if result
                    .host_snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.revision >= expected_host_revision)
                    && result.held_cpu_weight == 0
                    && result.held_memory_mib == 0
                    && result.held_disk_mib == 0
                    && result.held_process_slots == 0
                    && result.fairness_debt == 1
                    && result.reservations.iter().any(|summary| {
                        summary.reservation_id == RESERVATION
                            && summary.state == expected_state
                            && summary.tombstone
                    }) =>
            {
                return;
            }
            Ok(result) => last_error = format!("observed terminal status {result:?}"),
            Err(error) => last_error = error,
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!(
        "node {node_id} did not catch up terminal reservation state {expected_state:?}: {last_error}"
    );
}

/// Two independent public callers race the same claimed WorkItem attempt through
/// one real MultiRaft leader.  The distinct reservation identities must produce
/// exactly one accepted durable winner and one conflict; the public status read
/// then proves that only the winner charged the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_resource_public_dispatch_concurrent_attempt_race_has_one_winner() {
    let _auth_env_guard = configure_auth_test_environment(TENANT, "rmdd27-resource-public-auth");

    let mut cluster = ClusterGuard::new(
        Cluster::start(3, "rmdd27-resource-public-race")
            .await
            .expect("cluster starts"),
    );
    ensure_commons_graph(&cluster).await;
    attach_multi_raft(&cluster).await;
    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("initial leader elected");

    decode_host_result(dispatch_host_update(&cluster, leader, 30, 1).await, 1);
    seed_public_work_item_claim(&cluster, leader).await;

    let base_request = reservation_request(unix_ms(), None);
    let first_request = reservation_variant(
        &base_request,
        RACE_RESERVATION_A,
        "rmdd27-resource-race-idempotency-a",
    );
    let second_request = reservation_variant(
        &base_request,
        RACE_RESERVATION_B,
        "rmdd27-resource-race-idempotency-b",
    );
    let (first_response, second_response) = race_public_reservation_calls(
        &cluster,
        leader,
        RACE_AGENT_A,
        31,
        Method::ReserveWorkItemResources {
            request: first_request.clone(),
        },
        RACE_AGENT_B,
        32,
        Method::ReserveWorkItemResources {
            request: second_request.clone(),
        },
    )
    .await;
    let first: ResourceReservationResult = decode_raw(first_response, "concurrent reserve A");
    let second: ResourceReservationResult = decode_raw(second_response, "concurrent reserve B");

    assert!(
        (first.decision == ResourceReservationResultDecision::Accepted
            && second.decision == ResourceReservationResultDecision::Conflict)
            || (first.decision == ResourceReservationResultDecision::Conflict
                && second.decision == ResourceReservationResultDecision::Accepted),
        "same-attempt concurrent callers must yield one accepted winner and one conflict: first={first:?}, second={second:?}"
    );
    let (winner, loser, winner_request) =
        if first.decision == ResourceReservationResultDecision::Accepted {
            (&first, &second, &first_request)
        } else {
            (&second, &first, &second_request)
        };
    assert_eq!(winner.work_item_id, WORK_ITEM);
    assert_eq!(winner.state, ResourceReservationResultState::Reserved);
    assert_eq!(
        winner.reservation_id.as_deref(),
        Some(winner_request.reservation_id.as_str())
    );
    assert_eq!(winner.held_cpu_weight, 2);
    assert_eq!(winner.held_memory_mib, 1024);
    assert_eq!(winner.held_disk_mib, 200);
    assert_eq!(winner.held_process_slots, 1);
    assert_eq!(loser.work_item_id, WORK_ITEM);
    assert_eq!(loser.state, ResourceReservationResultState::Absent);
    assert_eq!(loser.held_cpu_weight, 0);
    assert_eq!(loser.held_memory_mib, 0);
    assert_eq!(loser.held_disk_mib, 0);
    assert_eq!(loser.held_process_slots, 0);

    // The exact public query is an authority ReadIndex read of the selected
    // winner.  An exact retry is idempotent, while the losing reservation has no
    // row and therefore cannot hide a second host charge.
    let queried = dispatch_method(
        &cluster,
        leader,
        33,
        reservation_query_method(winner_request, unix_ms()),
    )
    .await;
    let queried = decode_reservation_result_for(
        queried,
        "QueryWorkItemReservation after concurrent race",
        ResourceReservationResultDecision::Idempotent,
        ResourceReservationResultState::Reserved,
        winner_request.reservation_id.as_str(),
    );
    assert_eq!(queried.lifecycle_revision, winner.lifecycle_revision);
    assert_eq!(queried.held_cpu_weight, 2);

    let status = decode_status_result(dispatch_status(&cluster, leader, 34).await, 1);
    assert_active_status_for(&status, winner_request.reservation_id.as_str());

    cluster.finish().await;
}

/// Public result-producing native routing, authority ReadIndex, follower redirects,
/// leader failover, and restart catch-up in one bounded five-node cluster. Five
/// members keep a three-node quorum after the initial leader is killed and one
/// additional follower is isolated for the stale-read proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn native_resource_public_dispatch_readindex_and_failover() {
    let _auth_env_guard = configure_auth_test_environment(TENANT, "rmdd27-resource-public-auth");

    let mut cluster = ClusterGuard::new(
        Cluster::start(5, "rmdd27-resource-public")
            .await
            .expect("cluster starts"),
    );
    ensure_commons_graph(&cluster).await;
    attach_multi_raft(&cluster).await;
    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("initial leader elected");

    // The public signed dispatch boundary returns the exact native result produced
    // by the committed WorkItem-domain command, not a generic Raft acknowledgement.
    decode_host_result(dispatch_host_update(&cluster, leader, 1, 1).await, 1);
    let (reserve_request, reserved) = drive_public_work_item_resource_setup(&cluster, leader).await;

    let follower = cluster
        .all_ids()
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("a follower exists");
    // Wait only for durable follower apply before asserting the public redirect;
    // this direct backend poll is synchronization, not public evidence.
    wait_for_backend_revision(&cluster, follower, 1).await;
    wait_for_node_leader(&cluster, follower, leader).await;

    // Both a native write and a native authority read must route to the placement
    // leader.  The read leg is intentionally checked before failover so it cannot
    // be confused with a sealed-command roundtrip.
    assert_redirect(dispatch_host_update(&cluster, follower, 2, 2).await, leader);
    assert_redirect(dispatch_status(&cluster, follower, 3).await, leader);
    let status = decode_status_result(dispatch_status(&cluster, leader, 4).await, 1);
    assert_active_status(&status);

    cluster.kill(leader).await.expect("kill initial leader");
    let new_leader = cluster
        .wait_for_leader_excluding(leader, Duration::from_secs(20))
        .await
        .expect("survivor elected after leader failure");
    assert_ne!(new_leader, leader);
    // Election visibility and state-machine apply are separate observations in
    // the in-process harness. Wait for the active hold through the public
    // authority status path before releasing it; a host revision alone can
    // predate the reservation entry and does not prove the hold was applied.
    wait_for_public_active_status(&cluster, new_leader, 1).await;

    let stale_follower = cluster
        .live_ids()
        .into_iter()
        .find(|node_id| *node_id != new_leader)
        .expect("a follower remains after failover");
    // Establish that the selected follower has the pre-release hold and knows
    // the current leader before isolating it. The isolation leaves the second
    // survivor available, so the release still has a quorum to commit.
    wait_for_backend_active_reservation(&cluster, stale_follower, 1).await;
    wait_for_node_leader(&cluster, stale_follower, new_leader).await;
    crate::raft::network::partition::isolate(stale_follower);

    let released = drive_public_release_after_failover(
        &cluster,
        new_leader,
        &reserve_request,
        &reserved,
        stale_follower,
    )
    .await;

    // The isolated follower still has the old Reserved row, but its signed
    // public exact query is rejected with a leader redirect rather than being
    // allowed to authorize from that stale local state. Heal before issuing the
    // next committed mutation so the follower can catch up.
    crate::raft::network::partition::heal();

    // The new leader accepts a fresh result-producing native mutation and returns
    // its typed result; the surviving second follower applies the same row. The
    // host revision follows the release, so the restarted node must catch up both
    // the new telemetry and the terminal reservation tombstone.
    decode_host_result(dispatch_host_update(&cluster, new_leader, 5, 2).await, 2);
    let survivor = cluster
        .all_ids()
        .into_iter()
        .find(|node_id| *node_id != leader && *node_id != new_leader)
        .expect("one follower survives alongside the new leader");
    assert_eq!(survivor, stale_follower);
    // The direct follower poll only waits for catch-up.  The public leader status
    // read below is the evidence returned to an engine client.
    wait_for_backend_reservation_state(
        &cluster,
        stale_follower,
        2,
        ResourceReservationSummaryState::Released,
    )
    .await;
    decode_status_result(dispatch_status(&cluster, new_leader, 6).await, 2);

    // Restart the killed member over its original durable redb directory, attach
    // the restarted MultiRaft handle, and require it to catch up to revision 2.
    cluster
        .restart(leader)
        .await
        .expect("restart killed leader");
    ensure_commons_graph(&cluster).await;
    attach_multi_raft(&cluster).await;
    // Restart synchronization uses the backend poll only to wait for local apply;
    // the public status/exact-query assertions below establish served evidence.
    wait_for_backend_revision(&cluster, leader, 2).await;
    wait_for_backend_reservation_state(
        &cluster,
        leader,
        2,
        ResourceReservationSummaryState::Released,
    )
    .await;
    let current_leader = cluster
        .wait_for_leader(Duration::from_secs(20))
        .await
        .expect("current leader remains discoverable after restart");
    if current_leader == leader {
        wait_for_node_leader(&cluster, leader, leader).await;
        let status = decode_status_result(dispatch_status(&cluster, leader, 7).await, 2);
        assert_released_status(&status);
        let queried = dispatch_method(
            &cluster,
            leader,
            8,
            reservation_query_method(&reserve_request, unix_ms()),
        )
        .await;
        let queried = decode_reservation_result(
            queried,
            "public terminal query after restart",
            ResourceReservationResultDecision::Idempotent,
            ResourceReservationResultState::Released,
        );
        assert_eq!(queried.lifecycle_revision, released.lifecycle_revision);
    } else {
        wait_for_node_leader(&cluster, leader, current_leader).await;
        assert_redirect(dispatch_status(&cluster, leader, 7).await, current_leader);
        let status = decode_status_result(dispatch_status(&cluster, current_leader, 8).await, 2);
        assert_released_status(&status);
        let queried = dispatch_method(
            &cluster,
            current_leader,
            9,
            reservation_query_method(&reserve_request, unix_ms()),
        )
        .await;
        let queried = decode_reservation_result(
            queried,
            "public terminal query after restart",
            ResourceReservationResultDecision::Idempotent,
            ResourceReservationResultState::Released,
        );
        assert_eq!(queried.lifecycle_revision, released.lifecycle_revision);
    }

    cluster.finish().await;
}
