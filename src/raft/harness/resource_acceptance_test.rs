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

use std::future::Future;
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
use crate::server::{compute_verified_envelope_token, dispatch, VerifiedEnvelopeParams};

const SECRET: &str = "harness";
// `cfg(test)` request-context verification deliberately pins the shared harness
// tenant/policy.  Keep the resource rows unique by graph/IDs while honoring that
// public auth contract.
const TENANT: &str = "tenant-shared";
const HOST: &str = "rmdd27-resource-host";
const AUTH_AGENT: &str = "rmdd27-resource-public-test";
const DELEGATE_AGENT: &str = "rmdd27-delegate-selected-agent";
const CONFLICT_AGENT: &str = "rmdd27-resource-conflict-test";
const RACE_AGENT_A: &str = "rmdd27-resource-race-agent-a";
const RACE_AGENT_B: &str = "rmdd27-resource-race-agent-b";
const WORK_ITEM: &str = "rmdd27-resource-work-item";
const DELEGATE_WORK_ITEM: &str = "rmdd27-delegate-work-item";
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

// The debug-profile Raft/redb acceptance path legitimately exceeds libtest's
// small default worker stack while materializing several independent native
// stores. Keep the accommodation local to this heavy harness rather than making
// callers set RUST_MIN_STACK or changing production runtime configuration.
const CLUSTER_ACCEPTANCE_STACK_BYTES: usize = 16 * 1024 * 1024;
const CLUSTER_ACCEPTANCE_SCENARIO_TIMEOUT: Duration = Duration::from_secs(180);
const PUBLIC_DISPATCH_TIMEOUT: Duration = Duration::from_secs(15);

fn run_cluster_acceptance<F, Fut>(name: &'static str, scenario: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let outcome = std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(CLUSTER_ACCEPTANCE_STACK_BYTES)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(6)
                .enable_all()
                .build()
                .expect("build cluster acceptance runtime");
            runtime.block_on(async move {
                tokio::time::timeout(CLUSTER_ACCEPTANCE_SCENARIO_TIMEOUT, scenario())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "RMDD-27 cluster acceptance scenario exceeded {CLUSTER_ACCEPTANCE_SCENARIO_TIMEOUT:?}"
                        )
                    });
            });
        })
        .expect("spawn cluster acceptance thread")
        .join();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

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
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
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

#[cfg(feature = "redb")]
fn delegation_library_draft() -> eg_types::AgentLibraryEntryDraft {
    let prefixed = |seed: char| format!("sha256:{}", seed.to_string().repeat(64));
    let policy_digest =
        crate::server::persistence::agent_library::current_agent_library_policy_digest()
            .expect("Agent Library policy digest");
    eg_types::AgentLibraryEntryDraft {
        agent_id: DELEGATE_AGENT.to_string(),
        package_id: "rmdd27-delegate-package".to_string(),
        version: "1.0.0".to_string(),
        role: "worker".to_string(),
        role_digest: prefixed('1'),
        system_prompt: eg_types::agent_component::ComponentDependency {
            component_id: "cas:rmdd27-delegate-prompt".to_string(),
            kind: eg_types::agent_component::AgentComponentKind::SystemPrompt,
            definition_digest: prefixed('2'),
        },
        tools: vec![
            eg_types::agent_component::ComponentDependency {
                component_id: "tool:rmdd27-search".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::Tool,
                definition_digest: prefixed('3'),
            },
        ],
        skills: vec![
            eg_types::agent_component::ComponentDependency {
                component_id: "skill:rmdd27-delegate".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::Skill,
                definition_digest: prefixed('4'),
            },
        ],
        model_profile: eg_types::agent_component::ComponentDependency {
            component_id: "model-profile:rmdd27".to_string(),
            kind: eg_types::agent_component::AgentComponentKind::ModelProfile,
            definition_digest: prefixed('5'),
        },
        model_identity: "model:rmdd27".to_string(),
        ontologies: vec![
            eg_types::agent_component::ComponentDependency {
                component_id: "ontology:rmdd27".to_string(),
                kind: eg_types::agent_component::AgentComponentKind::Ontology,
                definition_digest: prefixed('6'),
            },
        ],
        tenant_id: TENANT.to_string(),
        actor_scope: "delegate:target".to_string(),
        purpose_id: "delegation.execute".to_string(),
        policy_digest,
        source_revision: "agent-library:rmdd27:1".to_string(),
        source_revision_digest: prefixed('7'),
        runtime: Default::default(),
        instantiated_from: None,
    }
}

#[cfg(feature = "redb")]
async fn seed_delegation_library(cluster: &Cluster) -> eg_types::AgentLibraryEntry {
    let draft = delegation_library_draft();
    let policy_digest = draft.policy_digest.clone();
    let caller_principal = crate::server::mutation_batch::principal_fingerprint(AUTH_AGENT)
        .expect("harness caller principal fingerprint");
    let mut retained = None;
    for node_id in cluster.all_ids() {
        let state = state_for(cluster, node_id);
        let store = state
            .write()
            .await
            .ensure_agent_library()
            .expect("open Agent Library owner");
        let context = eg_types::AgentLibraryMutationContext {
            request_id: 10_000 + node_id,
            principal: store.owner_principal().to_string(),
            caller_principal: caller_principal.clone(),
            attempt_nonce: eg_types::contract::Nonce::from_bytes([node_id as u8; 32]),
            tenant_id: TENANT.to_string(),
            actor_scope: "delegate:builder".to_string(),
            purpose_id: "agent-library:publish".to_string(),
            policy_revision: "policy-test".to_string(),
            policy_digest: policy_digest.clone(),
            policy_decision_id: format!("rmdd27-delegate-policy-{node_id}"),
            idempotency_key: format!("rmdd27-delegate-library-seed-{node_id}"),
            expected_revision: Some(0),
            trace_id: Some(format!("rmdd27-delegate-library-trace-{node_id}")),
            created_at_ms: 1,
        };
        let result = store
            .publish(eg_types::AgentLibraryPublishRequest {
                context,
                entry: draft.clone(),
            })
            .expect("publish selected Agent Library revision");
        if let Some(previous) = retained.as_ref() {
            assert_eq!(previous, &result.entry);
        } else {
            retained = Some(result.entry);
        }
    }
    retained.expect("cluster has at least one retained Agent Library entry")
}

#[cfg(feature = "redb")]
async fn advance_delegation_library(cluster: &Cluster, retained: &eg_types::AgentLibraryEntry) {
    let mut draft = retained.as_draft();
    draft.version = "2.0.0".to_string();
    draft.source_revision = "agent-library:rmdd27:2".to_string();
    draft.source_revision_digest =
        "sha256:8888888888888888888888888888888888888888888888888888888888888888".to_string();
    let policy_digest = draft.policy_digest.clone();
    let caller_principal = crate::server::mutation_batch::principal_fingerprint(AUTH_AGENT)
        .expect("harness caller principal fingerprint");
    for node_id in cluster.all_ids() {
        let state = state_for(cluster, node_id);
        let store = state
            .write()
            .await
            .ensure_agent_library()
            .expect("open Agent Library owner for revision advance");
        let result = store
            .publish(eg_types::AgentLibraryPublishRequest {
                context: eg_types::AgentLibraryMutationContext {
                    request_id: 11_000 + node_id,
                    principal: store.owner_principal().to_string(),
                    caller_principal: caller_principal.clone(),
                    attempt_nonce: eg_types::contract::Nonce::from_bytes([100 + node_id as u8; 32]),
                    tenant_id: TENANT.to_string(),
                    actor_scope: "delegate:builder".to_string(),
                    purpose_id: "agent-library:publish".to_string(),
                    policy_revision: "policy-test".to_string(),
                    policy_digest: policy_digest.clone(),
                    policy_decision_id: format!("rmdd27-delegate-policy-advance-{node_id}"),
                    idempotency_key: format!("rmdd27-delegate-library-advance-{node_id}"),
                    expected_revision: Some(retained.entry_revision),
                    trace_id: Some(format!("rmdd27-delegate-library-advance-trace-{node_id}")),
                    created_at_ms: 2,
                },
                entry: draft.clone(),
            })
            .expect("publish next retained Agent Library revision");
        assert_eq!(result.entry.entry_revision, retained.entry_revision + 1);
        assert_ne!(result.entry.definition_digest, retained.definition_digest);
    }
}

#[cfg(feature = "redb")]
async fn retire_delegation_library(cluster: &Cluster, retained: &eg_types::AgentLibraryEntry) {
    let policy_digest =
        crate::server::persistence::agent_library::current_agent_library_policy_digest()
            .expect("Agent Library policy digest for retirement");
    let caller_principal = crate::server::mutation_batch::principal_fingerprint(AUTH_AGENT)
        .expect("harness caller principal fingerprint");
    let expected_revision = retained.entry_revision + 1;
    for node_id in cluster.all_ids() {
        let state = state_for(cluster, node_id);
        let store = state
            .write()
            .await
            .ensure_agent_library()
            .expect("open Agent Library owner for retirement");
        let result = store
            .retire(eg_types::AgentLibraryRetireRequest {
                context: eg_types::AgentLibraryMutationContext {
                    request_id: 12_000 + node_id,
                    principal: store.owner_principal().to_string(),
                    caller_principal: caller_principal.clone(),
                    attempt_nonce: eg_types::contract::Nonce::from_bytes([150 + node_id as u8; 32]),
                    tenant_id: TENANT.to_string(),
                    actor_scope: "delegate:builder".to_string(),
                    purpose_id: "agent-library:retire".to_string(),
                    policy_revision: "policy-test".to_string(),
                    policy_digest: policy_digest.clone(),
                    policy_decision_id: format!("rmdd27-delegate-policy-retire-{node_id}"),
                    idempotency_key: format!("rmdd27-delegate-library-retire-{node_id}"),
                    expected_revision: Some(expected_revision),
                    trace_id: Some(format!("rmdd27-delegate-library-retire-trace-{node_id}")),
                    created_at_ms: 3,
                },
                agent_id: retained.agent_id.clone(),
            })
            .expect("retire selected Agent Library head");
        assert!(result.entry.is_retired());
        assert_eq!(result.entry.entry_revision, expected_revision + 1);
    }
}

#[cfg(feature = "redb")]
fn delegation_method(entry: &eg_types::AgentLibraryEntry) -> Method {
    let now_ms = unix_ms();
    let context = eg_types::epistemic_operations::RequestContext {
        schema_version: eg_types::epistemic_operations::RequestContextSchemaVersion::V2,
        request_id: "rmdd27-delegate-context-request".to_string(),
        subject_id: AUTH_AGENT.to_string(),
        tenant_id: TENANT.to_string(),
        agent_id: AUTH_AGENT.to_string(),
        scopes: vec!["work:delegate".to_string()],
        audience: "epistemic-graph-test".to_string(),
        authentication_method:
            eg_types::epistemic_operations::RequestContextAuthenticationMethod::LocalProcess,
        policy_version: "policy-test".to_string(),
        graph: GRAPH.to_string(),
        placement_epoch: None,
        trace_id: "rmdd27-delegate-context-trace".to_string(),
        issued_at_ms: now_ms,
        expires_at_ms: now_ms.saturating_add(60_000),
    };
    let raw_digest = |field: &str| {
        field
            .strip_prefix("sha256:")
            .expect("Agent Library fixture uses prefixed digests")
            .to_string()
    };
    Method::KgDelegate {
        request: Box::new(eg_types::KgDelegateRequest {
            schema_version: eg_types::KgDelegateSchemaVersion::V1,
            context,
            delegation_id: "rmdd27-delegation".to_string(),
            run_id: "rmdd27-run".to_string(),
            trace_id: "rmdd27-trace".to_string(),
            target: eg_types::delegation::DelegationTarget::Agent {
                entry: eg_types::AgentLibraryEntryRef::from_entry(entry),
            },
            input_ref: "cas:rmdd27-delegate-input".to_string(),
            command_digest: IMMUTABLE_DIGEST.to_string(),
            capability_digest: raw_digest(&entry.tool_set_digest()),
            catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.to_string(),
            policy_digest: entry.policy_digest.clone(),
            model_digest: Some(raw_digest(&entry.model_profile_digest())),
            idempotency_key: "rmdd27-delegation-idempotency".to_string(),
            kind: "agent.execute".to_string(),
            actor_scope: entry.actor_scope.clone(),
            purpose: entry.purpose_id.clone(),
            work_item_id: Some(DELEGATE_WORK_ITEM.to_string()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some((now_ms / 1_000 + 60) as f64),
            max_tenant_in_flight: 10,
        }),
    }
}

#[cfg(feature = "redb")]
fn replayable_delegation_method(
    entry: &eg_types::AgentLibraryEntry,
    deadline_unix: Option<f64>,
) -> Method {
    let mut method = delegation_method(entry);
    if let Method::KgDelegate { request } = &mut method {
        request.deadline_unix = deadline_unix;
    }
    method
}

#[cfg(feature = "redb")]
fn decode_delegation_result(response: crate::protocol::Response) -> eg_types::KgDelegateResult {
    assert!(response.error.is_none(), "delegation failed: {response:?}");
    let payload = response.result.expect("delegation result");
    let bytes = match payload {
        ResultPayload::Raw(bytes) => bytes,
        other => panic!("delegation result has unexpected payload: {other:?}"),
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode typed kg-delegate result")
}

#[cfg(feature = "redb")]
fn seed_restart_delegate_entry(persist_dir: &str) -> eg_types::AgentLibraryEntry {
    use eg_types::agent_library::{AgentLibraryEntryDraft, AgentLibraryMutationContext};
    use eg_types::contract::Nonce;

    let digest = |seed: char| format!("sha256:{}", seed.to_string().repeat(64));
    let store = crate::server::persistence::agent_library::AgentLibraryStore::open(persist_dir)
        .expect("open durable Agent Library owner for restart fixture");
    let policy_digest =
        crate::server::persistence::agent_library::current_agent_library_policy_digest()
            .expect("derive Agent Library policy digest");
    let entry = store
        .publish(eg_types::AgentLibraryPublishRequest {
            context: AgentLibraryMutationContext {
                request_id: 90_001,
                principal: store.owner_principal().to_string(),
                caller_principal: format!("principal:sha256:{}", "a".repeat(64)),
                attempt_nonce: Nonce::from_bytes([0x91; 32]),
                tenant_id: TENANT.to_string(),
                actor_scope: "action-scope:agent-library-publish".to_string(),
                purpose_id: "agent-library:publish".to_string(),
                policy_revision: "policy-test".to_string(),
                policy_digest,
                policy_decision_id: "rmdd27-agent-library-publish".to_string(),
                idempotency_key: "rmdd27-agent-library-publish".to_string(),
                expected_revision: Some(0),
                trace_id: Some("rmdd27-agent-library-publish-trace".to_string()),
                created_at_ms: unix_ms(),
            },
            entry: AgentLibraryEntryDraft {
                agent_id: "rmdd27-selected-agent".to_string(),
                package_id: "rmdd27-agent-package".to_string(),
                version: "1.0.0".to_string(),
                role: "worker".to_string(),
                role_digest: digest('a'),
                system_prompt: eg_types::agent_component::ComponentDependency {
                    component_id: "prompt:rmdd27-agent-v1".to_string(),
                    kind: eg_types::agent_component::AgentComponentKind::SystemPrompt,
                    definition_digest: digest('b'),
                },
                tools: vec![
                    eg_types::agent_component::ComponentDependency {
                        component_id: "tool:rmdd27-search".to_string(),
                        kind: eg_types::agent_component::AgentComponentKind::Tool,
                        definition_digest: digest('c'),
                    },
                ],
                skills: vec![
                    eg_types::agent_component::ComponentDependency {
                        component_id: "skill:rmdd27-reason".to_string(),
                        kind: eg_types::agent_component::AgentComponentKind::Skill,
                        definition_digest: digest('d'),
                    },
                ],
                model_profile: eg_types::agent_component::ComponentDependency {
                    component_id: "model-profile:rmdd27-default".to_string(),
                    kind: eg_types::agent_component::AgentComponentKind::ModelProfile,
                    definition_digest: digest('e'),
                },
                model_identity: "model:rmdd27-default".to_string(),
                ontologies: vec![
                    eg_types::agent_component::ComponentDependency {
                        component_id: "ontology:rmdd27-core".to_string(),
                        kind: eg_types::agent_component::AgentComponentKind::Ontology,
                        definition_digest: digest('f'),
                    },
                ],
                tenant_id: TENANT.to_string(),
                actor_scope: "definition:rmdd27-selected-agent".to_string(),
                purpose_id: "delegation.execute".to_string(),
                policy_digest: digest('0'),
                source_revision: "rmdd27-agent-source:1".to_string(),
                source_revision_digest: digest('1'),
                runtime: Default::default(),
                instantiated_from: None,
            },
        })
        .expect("publish retained Agent Library definition")
        .entry;
    drop(store);
    entry
}

#[cfg(feature = "redb")]
fn restart_delegate_method(entry: &eg_types::AgentLibraryEntry) -> Method {
    let raw_digest = |seed: char| seed.to_string().repeat(64);
    let now_ms = unix_ms();
    let agent_entry = eg_types::AgentLibraryEntryRef::from_entry(entry);
    let unprefixed = |digest: &str| digest.strip_prefix("sha256:").unwrap().to_string();
    Method::KgDelegate {
        request: Box::new(eg_types::KgDelegateRequest {
            schema_version: eg_types::KgDelegateSchemaVersion::V1,
            context: eg_types::epistemic_operations::RequestContext {
                schema_version: eg_types::epistemic_operations::RequestContextSchemaVersion::V2,
                request_id: "rmdd27-restart-delegate-request".to_string(),
                subject_id: "rmdd27-restart-delegate-subject".to_string(),
                tenant_id: TENANT.to_string(),
                agent_id: AUTH_AGENT.to_string(),
                scopes: vec!["work:delegate".to_string()],
                audience: "epistemic-graph-test".to_string(),
                authentication_method:
                    eg_types::epistemic_operations::RequestContextAuthenticationMethod::LocalProcess,
                policy_version: "policy-test".to_string(),
                graph: GRAPH.to_string(),
                placement_epoch: None,
                trace_id: "rmdd27-restart-delegate-context-trace".to_string(),
                issued_at_ms: now_ms,
                expires_at_ms: now_ms.saturating_add(60_000),
            },
            delegation_id: "rmdd27-restart-delegation".to_string(),
            run_id: "rmdd27-restart-run".to_string(),
            trace_id: "rmdd27-restart-trace".to_string(),
            target: eg_types::delegation::DelegationTarget::Agent { entry: agent_entry },
            input_ref: "cas:rmdd27-restart-input".to_string(),
            command_digest: raw_digest('2'),
            capability_digest: unprefixed(&entry.tool_set_digest()),
            catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.to_string(),
            policy_digest: entry.policy_digest.clone(),
            model_digest: Some(unprefixed(&entry.model_profile_digest())),
            idempotency_key: "rmdd27-restart-delegate-idempotency".to_string(),
            kind: "agent.execute".to_string(),
            actor_scope: entry.actor_scope.clone(),
            purpose: entry.purpose_id.clone(),
            work_item_id: Some("rmdd27-restart-delegate-work-item".to_string()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some((now_ms / 1_000 + 60) as f64),
            max_tenant_in_flight: 10,
        }),
    }
}

#[cfg(feature = "redb")]
fn missing_authority_delegate_method() -> Method {
    let raw_digest = |seed: char| seed.to_string().repeat(64);
    let prefixed_digest = |seed: char| format!("sha256:{}", raw_digest(seed));
    let now_ms = unix_ms();
    Method::KgDelegate {
        request: Box::new(eg_types::KgDelegateRequest {
            schema_version: eg_types::KgDelegateSchemaVersion::V1,
            context: eg_types::epistemic_operations::RequestContext {
                schema_version: eg_types::epistemic_operations::RequestContextSchemaVersion::V2,
                request_id: "rmdd27-missing-authority-request".to_string(),
                subject_id: "rmdd27-missing-authority-subject".to_string(),
                tenant_id: TENANT.to_string(),
                agent_id: AUTH_AGENT.to_string(),
                scopes: vec!["work:delegate".to_string()],
                audience: "epistemic-graph-test".to_string(),
                authentication_method:
                    eg_types::epistemic_operations::RequestContextAuthenticationMethod::LocalProcess,
                policy_version: "policy-test".to_string(),
                graph: GRAPH.to_string(),
                placement_epoch: None,
                trace_id: "rmdd27-missing-authority-context-trace".to_string(),
                issued_at_ms: now_ms,
                expires_at_ms: now_ms.saturating_add(60_000),
            },
            delegation_id: "rmdd27-missing-authority-delegation".to_string(),
            run_id: "rmdd27-missing-authority-run".to_string(),
            trace_id: "rmdd27-missing-authority-trace".to_string(),
            target: eg_types::delegation::DelegationTarget::Agent {
                entry: eg_types::AgentLibraryEntryRef {
                    tenant_id: TENANT.to_string(),
                    agent_id: "rmdd27-selected-agent".to_string(),
                    entry_revision: 1,
                    definition_digest: prefixed_digest('a'),
                    source_revision: "agent-library:rmdd27:1".to_string(),
                    source_revision_digest: prefixed_digest('b'),
                    actor_scope: "delegate:target".to_string(),
                    purpose_id: "delegation.execute".to_string(),
                    policy_digest: prefixed_digest('c'),
                },
            },
            input_ref: "cas:rmdd27-missing-authority-input".to_string(),
            command_digest: raw_digest('d'),
            capability_digest: raw_digest('e'),
            catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.to_string(),
            policy_digest: prefixed_digest('c'),
            model_digest: Some(raw_digest('f')),
            idempotency_key: "rmdd27-missing-authority-idempotency".to_string(),
            kind: "agent.execute".to_string(),
            actor_scope: "delegate:target".to_string(),
            purpose: "delegation.execute".to_string(),
            work_item_id: Some("rmdd27-missing-authority-work-item".to_string()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some((now_ms / 1_000 + 60) as f64),
            max_tenant_in_flight: 10,
        }),
    }
}

#[cfg(feature = "redb")]
fn missing_authority_delegate_batch_id() -> String {
    native_work_item_batch_id("rmdd27-missing-authority-idempotency")
}

#[cfg(feature = "redb")]
fn native_work_item_batch_id(idempotency_key: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph.work-item-terminal.v1");
    for field in [
        GRAPH.as_bytes(),
        TENANT.as_bytes(),
        idempotency_key.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("work:{}", hex::encode(digest.finalize()))
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
        let mut guard = state.write().await;
        let raft = guard.raft.clone();
        guard.install_multi_raft_placement_authority(raft, multi);
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

/// BUG-044-class: keep the full (`--features full`) dispatcher's state machine
/// behind one heap indirection. `dispatch()` bottoms out in `dispatch_inner`
/// (`src/server/dispatch.rs`), one very large async fn whose generated future is
/// enormous; every caller in this file funnels through `dispatch_bounded`, itself
/// several `async fn` layers deep (`dispatch_method`/`dispatch_method_as` →
/// `dispatch_bounded` → `timeout(dispatch(..))`), so without boxing the huge future
/// gets embedded inline at every layer and can exhaust the test worker stack before
/// the first request is even polled. Mirrors `server::mod::tests::dispatch_on_heap`
/// (8e00e0b).
fn dispatch_on_heap<'a>(
    state: &'a Arc<RwLock<crate::server::ServerState>>,
    request: Request,
) -> std::pin::Pin<Box<dyn Future<Output = crate::protocol::Response> + Send + 'a>> {
    Box::pin(dispatch(state, request))
}

async fn dispatch_bounded(
    state: &Arc<RwLock<crate::server::ServerState>>,
    request: Request,
) -> crate::protocol::Response {
    let request_id = request.id;
    tokio::time::timeout(PUBLIC_DISPATCH_TIMEOUT, dispatch_on_heap(state, request))
        .await
        .unwrap_or_else(|_| {
            panic!("public dispatch request {request_id} exceeded {PUBLIC_DISPATCH_TIMEOUT:?}")
        })
}

async fn dispatch_host_update(
    cluster: &Cluster,
    node_id: NodeId,
    request_id: u64,
    revision: u64,
) -> crate::protocol::Response {
    let now_ms = unix_ms();
    dispatch_bounded(
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
    dispatch_bounded(
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
// Private test-harness helper: paired (agent, request_id, method) triples for
// the two racing callers plus the cluster/leader context, each independently
// required; no natural grouping beyond re-introducing the same fields as a
// throwaway pair-struct.
#[allow(clippy::too_many_arguments)]
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
        dispatch_bounded(&first_state, first_request).await
    });
    let second_barrier = barrier.clone();
    let second_task = tokio::spawn(async move {
        second_barrier.wait().await;
        dispatch_bounded(&second_state, second_request).await
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

#[cfg(feature = "redb")]
async fn transfer_graph_leader(cluster: &Cluster, from: NodeId, target: NodeId) {
    let state = state_for(cluster, from);
    let multi = state
        .read()
        .await
        .multi_raft
        .clone()
        .expect("reopened member has MultiRaft authority");
    let routed = multi
        .handle_for_graph(GRAPH)
        .await
        .expect("reopened member has the graph Raft group");
    assert_eq!(
        routed.handle.current_leader().await,
        Some(from),
        "leadership transfer must start from the current leader"
    );
    routed
        .handle
        .raft
        .trigger()
        .transfer_leader(target)
        .await
        .expect("transfer graph leadership to the reopened member");
    wait_for_node_leader(cluster, target, target).await;
}

fn decode_host_result(response: crate::protocol::Response, expected_revision: u64) {
    let result: ResourceHostUpdateResult = decode_raw(response, "native update");
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
    let bytes = match response.result.as_ref() {
        // These two untagged byte variants are wire-identical and either may be
        // selected when the durable outer ResultPayload is decoded.
        Some(ResultPayload::Raw(bytes)) => bytes,
        _ => panic!("{label} did not return a typed byte result: {response:?}"),
    };
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .unwrap_or_else(|error| panic!("decode {label} result: {error:?}"))
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
    let result: ResourceReservationStatusResult = decode_raw(response, "native status");
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
    let bytes = match response.result.as_ref() {
        Some(ResultPayload::Raw(bytes)) => bytes,
        _ => panic!("redirect did not carry the structured operation result: {response:?}"),
    };
    let detail: OperationResult = eg_types::msgpack::decode_bounded(
        bytes,
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

#[cfg(feature = "redb")]
async fn wait_for_graph_node(cluster: &Cluster, node_id: NodeId, node: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if cluster.has_node_in(node_id, GRAPH, node).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!("node {node_id} did not apply graph node '{node}'");
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
                        Err(error) => last_error = format!("decode public status: {error:?}"),
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

    // A public transport retry returns the exact first durable result (Accepted),
    // not a second host hold or a generic successful Raft acknowledgement. The
    // separate authority query below uses the domain-level Idempotent decision.
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
        ResourceReservationResultDecision::Accepted,
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
    // Reserve and release are distinct durable commands. Their caller-stable
    // transport identities must not alias even though they address one record.
    release_request.idempotency_key =
        "rmdd27-resource-release-after-failover-idempotency".to_string();
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
#[test]
fn native_resource_public_dispatch_concurrent_attempt_race_has_one_winner() {
    run_cluster_acceptance(
        "rmdd27-resource-public-race",
        native_resource_public_dispatch_concurrent_attempt_race_scenario,
    );
}

async fn native_resource_public_dispatch_concurrent_attempt_race_scenario() {
    let _auth_env_guard = configure_auth_test_environment(TENANT, "rmdd27-resource-public-auth");
    let _partition_guard = crate::raft::network::partition::test_guard();

    let cluster = ClusterGuard::new(
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
#[test]
fn native_resource_public_dispatch_readindex_and_failover() {
    run_cluster_acceptance(
        "rmdd27-resource-public-failover",
        native_resource_public_dispatch_readindex_and_failover_scenario,
    );
}

async fn native_resource_public_dispatch_readindex_and_failover_scenario() {
    let _auth_env_guard = configure_auth_test_environment(TENANT, "rmdd27-resource-public-auth");
    let _partition_guard = crate::raft::network::partition::test_guard();

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

/// RF-020 admission must use the same signed native WorkItem route as every
/// other clustered write.  This drives a real KgDelegate request through a
/// follower redirect, leader proposal, replicated apply, leader failure, and
/// fresh-nonce replay.  The final conflicting payload proves the native
/// idempotency record remains the authority after failover.
#[cfg(feature = "redb")]
#[test]
fn kg_delegate_public_dispatch_replicates_and_replays_after_failover() {
    run_cluster_acceptance(
        "rmdd27-kg-delegate-public-failover",
        kg_delegate_public_dispatch_replicates_and_replays_after_failover_scenario,
    );
}

#[cfg(feature = "redb")]
async fn kg_delegate_public_dispatch_replicates_and_replays_after_failover_scenario() {
    let _auth_env_guard = configure_auth_test_environment(TENANT, "rmdd27-kg-delegate-public-auth");
    let _partition_guard = crate::raft::network::partition::test_guard();
    let mut cluster = ClusterGuard::new(
        Cluster::start(3, "rmdd27-kg-delegate-public")
            .await
            .expect("cluster starts"),
    );
    ensure_commons_graph(&cluster).await;
    let entry = seed_delegation_library(&cluster).await;
    attach_multi_raft(&cluster).await;
    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("initial leader elected");

    let accepted_method = delegation_method(&entry);
    let accepted_deadline = match &accepted_method {
        Method::KgDelegate { request } => request.deadline_unix,
        _ => unreachable!(),
    };
    let accepted = decode_delegation_result(
        dispatch_method(&cluster, leader, 1, accepted_method.clone()).await,
    );
    assert_eq!(accepted.decision, eg_types::KgDelegateDecision::Accepted);
    assert_eq!(accepted.work_item_id, DELEGATE_WORK_ITEM);

    // The native idempotency key is stable across fresh authenticated envelopes,
    // while an identical signed envelope is rejected by the outer nonce ledger.
    let same_envelope = signed_request_as(5, AUTH_AGENT, accepted_method.clone());
    let native_replay = decode_delegation_result(
        dispatch_bounded(&state_for(&cluster, leader), same_envelope.clone()).await,
    );
    assert_eq!(
        native_replay.decision,
        eg_types::KgDelegateDecision::Replayed
    );
    let nonce_replay = dispatch_bounded(&state_for(&cluster, leader), same_envelope).await;
    assert_eq!(
        nonce_replay.error.as_deref(),
        Some("nonce already used (replay rejected)"),
        "the exact signed KgDelegate envelope must be rejected before native replay"
    );

    let follower = cluster
        .all_ids()
        .into_iter()
        .find(|node_id| *node_id != leader)
        .expect("a follower exists");
    wait_for_graph_node(&cluster, follower, DELEGATE_WORK_ITEM).await;
    wait_for_node_leader(&cluster, follower, leader).await;
    assert_redirect(
        dispatch_method(&cluster, follower, 2, accepted_method.clone()).await,
        leader,
    );
    assert!(
        cluster
            .has_node_in(follower, GRAPH, DELEGATE_WORK_ITEM)
            .await,
        "follower must apply the acknowledged native WorkItem admission"
    );

    // Advance the selected definition independently on every member.  The
    // replay below still names B's original immutable revision; a handler that
    // resolves only the current head would reject this otherwise valid retry.
    advance_delegation_library(&cluster, &entry).await;
    retire_delegation_library(&cluster, &entry).await;

    // A fresh stable key cannot pin an old published revision once the current
    // Agent Library head is retired.  This refusal must happen before native
    // WorkItem admission, so it cannot create a second receipt or outbox row.
    let mut retired_old_revision = replayable_delegation_method(&entry, accepted_deadline);
    if let Method::KgDelegate { request } = &mut retired_old_revision {
        request.delegation_id = "rmdd27-retired-old-revision-delegation".to_string();
        request.run_id = "rmdd27-retired-old-revision-run".to_string();
        request.trace_id = "rmdd27-retired-old-revision-trace".to_string();
        request.idempotency_key = "rmdd27-retired-old-revision-idempotency".to_string();
        request.work_item_id = Some("rmdd27-retired-old-revision-work-item".to_string());
    }
    let refused = dispatch_method(&cluster, leader, 3, retired_old_revision).await;
    assert!(
        refused
            .error
            .as_deref()
            .is_some_and(|error| error.contains("head is retired")),
        "a fresh key pinned to an old revision must refuse after retirement: {refused:?}"
    );
    assert!(
        !cluster
            .has_node_in(leader, GRAPH, "rmdd27-retired-old-revision-work-item")
            .await,
        "retired old revision refusal must not write a WorkItem"
    );
    let leader_backend = state_for(&cluster, leader)
        .read()
        .await
        .persistence
        .clone()
        .expect("leader has persistence");
    assert!(
        leader_backend
            .read_mutation_batch(
                &crate::persist::sanitize(GRAPH),
                &native_work_item_batch_id("rmdd27-retired-old-revision-idempotency"),
            )
            .await
            .expect("read refused retired-key receipt")
            .is_none(),
        "retired old revision refusal must not write a native receipt"
    );
    assert!(
        leader_backend
            .read_mutation_outbox(
                &crate::persist::sanitize(GRAPH),
                &native_work_item_batch_id("rmdd27-retired-old-revision-idempotency"),
            )
            .await
            .expect("read refused retired-key outbox")
            .is_empty(),
        "retired old revision refusal must not write a native outbox row"
    );

    cluster.kill(leader).await.expect("kill initial leader");
    let new_leader = cluster
        .wait_for_leader_excluding(leader, Duration::from_secs(20))
        .await
        .expect("survivor elected after leader failure");
    let replayed = decode_delegation_result(
        dispatch_method(
            &cluster,
            new_leader,
            4,
            replayable_delegation_method(&entry, accepted_deadline),
        )
        .await,
    );
    assert_eq!(replayed.decision, eg_types::KgDelegateDecision::Replayed);
    assert_eq!(replayed.work_item_id, accepted.work_item_id);
    assert_eq!(replayed.outbox_id, accepted.outbox_id);

    let mut conflict = replayable_delegation_method(&entry, accepted_deadline);
    if let Method::KgDelegate { request } = &mut conflict {
        request.command_digest = "b".repeat(64);
    }
    let conflict = dispatch_method(&cluster, new_leader, 5, conflict).await;
    assert!(
        conflict
            .error
            .as_deref()
            .is_some_and(|error| error.contains("IDEMPOTENCY_CONFLICT")),
        "changed payload under the native key must conflict: {conflict:?}"
    );

    // Reopen the killed member over its original redb and prove that the
    // replicated WorkItem and native replay receipt survive process recovery.
    cluster
        .restart(leader)
        .await
        .expect("restart the original leader over durable redb");
    ensure_commons_graph(&cluster).await;
    attach_multi_raft(&cluster).await;
    wait_for_graph_node(&cluster, leader, DELEGATE_WORK_ITEM).await;
    wait_for_node_leader(&cluster, leader, new_leader).await;
    assert!(
        cluster.has_node_in(leader, GRAPH, DELEGATE_WORK_ITEM).await,
        "reopened member must retain the replicated WorkItem"
    );
    transfer_graph_leader(&cluster, new_leader, leader).await;
    wait_for_node_leader(&cluster, new_leader, leader).await;
    let reopened = decode_delegation_result(
        dispatch_method(
            &cluster,
            leader,
            6,
            replayable_delegation_method(&entry, accepted_deadline),
        )
        .await,
    );
    assert_eq!(reopened.decision, eg_types::KgDelegateDecision::Replayed);
    assert_eq!(reopened.work_item_id, accepted.work_item_id);
    assert_eq!(reopened.outbox_id, accepted.outbox_id);
    assert_eq!(reopened.command_digest, accepted.command_digest);
    assert_eq!(reopened.target, accepted.target);

    let backend = state_for(&cluster, leader)
        .read()
        .await
        .persistence
        .clone()
        .expect("reopened member has persistence");
    let graph_fname = crate::persist::sanitize(GRAPH);
    let record = backend
        .read_mutation_batch(
            &graph_fname,
            &native_work_item_batch_id("rmdd27-delegation-idempotency"),
        )
        .await
        .expect("read reopened native delegation receipt")
        .expect("reopened native delegation receipt exists");
    let native_payload: ResultPayload = eg_types::msgpack::decode_bounded(
        record
            .result_msgpack
            .as_ref()
            .expect("native receipt result"),
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode reopened native result envelope");
    let native: eg_types::native_control::SubmitWorkItemResult = match native_payload {
        ResultPayload::Raw(bytes) => eg_types::msgpack::decode_bounded(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
        )
        .expect("decode reopened native delegation receipt"),
        ResultPayload::Json(value) => {
            serde_json::from_value(value).expect("decode reopened native JSON receipt")
        }
        other => panic!("reopened native result has unexpected shape: {other:?}"),
    };
    assert_eq!(native.work_item_id, accepted.work_item_id);
    assert_eq!(native.outbox_id, accepted.outbox_id);
    assert_eq!(native.command_digest, accepted.command_digest);
    assert_eq!(native.idempotency_key, "rmdd27-delegation-idempotency");
    let request = match accepted_method {
        Method::KgDelegate { request } => request,
        _ => unreachable!(),
    };
    assert_eq!(
        native.provenance_refs,
        crate::server::handlers::delegation::provenance_refs(&request),
        "reopened receipt must retain the exact delegation provenance"
    );

    cluster.finish().await;
}

/// The first signed KgDelegate after a node restart must reopen the durable
/// Agent Library owner itself. The fixture seeds the retained definition before
/// the crash, then deliberately leaves the reopened ServerState's in-memory
/// `agent_library` handle unset until this public route is dispatched.
#[cfg(feature = "redb")]
#[test]
fn kg_delegate_public_dispatch_first_request_after_restart() {
    run_cluster_acceptance(
        "rmdd27-kg-delegate-public-first-after-restart",
        kg_delegate_public_dispatch_first_request_after_restart_scenario,
    );
}

#[cfg(feature = "redb")]
async fn kg_delegate_public_dispatch_first_request_after_restart_scenario() {
    let _auth_env_guard =
        configure_auth_test_environment(TENANT, "rmdd27-kg-delegate-public-restart-auth");
    let _partition_guard = crate::raft::network::partition::test_guard();
    let mut cluster = ClusterGuard::new(
        Cluster::start(1, "rmdd27-kg-delegate-public-first-after-restart")
            .await
            .expect("cluster starts"),
    );
    ensure_commons_graph(&cluster).await;
    let node_id = cluster
        .all_ids()
        .into_iter()
        .next()
        .expect("single-node cluster has a node");
    let persist_dir = state_for(&cluster, node_id)
        .read()
        .await
        .persist_dir
        .clone()
        .expect("restart fixture has a durable persist directory");
    let entry = seed_restart_delegate_entry(&persist_dir);
    assert!(
        state_for(&cluster, node_id)
            .read()
            .await
            .agent_library
            .is_none(),
        "seed setup must not initialize the ServerState library handle"
    );

    cluster.kill(node_id).await.expect("kill node for restart");
    cluster.restart(node_id).await.expect("restart node");
    ensure_commons_graph(&cluster).await;
    assert!(
        state_for(&cluster, node_id)
            .read()
            .await
            .agent_library
            .is_none(),
        "reopened state must begin without an in-memory Agent Library handle"
    );
    let leader = cluster
        .wait_for_leader(Duration::from_secs(15))
        .await
        .expect("restarted single-node cluster elects a leader");
    let response = dispatch_method(&cluster, leader, 1, restart_delegate_method(&entry)).await;
    let result: eg_types::KgDelegateResult = decode_raw(response, "KgDelegate after restart");
    assert_eq!(
        result.decision,
        eg_types::KgDelegateDecision::Accepted,
        "first post-restart delegation must commit through native admission"
    );
    assert_eq!(
        result.target,
        eg_types::delegation::DelegationTarget::Agent {
            entry: eg_types::AgentLibraryEntryRef::from_entry(&entry),
        }
    );
    assert!(
        state_for(&cluster, node_id)
            .read()
            .await
            .agent_library
            .is_some(),
        "first KgDelegate must lazily attach the durable Agent Library owner"
    );
    assert!(
        cluster
            .has_node_in(node_id, GRAPH, "rmdd27-restart-delegate-work-item")
            .await,
        "native KgDelegate admission must materialize the WorkItem"
    );

    cluster.finish().await;
}

/// A signed KgDelegate request must fail closed when the process is configured
/// as clustered but its MultiRaft placement authority is unavailable. The
/// explicit missing composition must not fall through to a local WorkItem
/// commit or leave a receipt/outbox row behind.
#[cfg(feature = "redb")]
#[test]
fn kg_delegate_public_dispatch_rejects_missing_cluster_authority() {
    run_cluster_acceptance(
        "rmdd27-kg-delegate-public-missing-authority",
        kg_delegate_public_dispatch_rejects_missing_cluster_authority_scenario,
    );
}

#[cfg(feature = "redb")]
async fn kg_delegate_public_dispatch_rejects_missing_cluster_authority_scenario() {
    let _auth_env_guard =
        configure_auth_test_environment(TENANT, "rmdd27-kg-delegate-public-missing-authority-auth");
    let _partition_guard = crate::raft::network::partition::test_guard();
    let cluster = ClusterGuard::new(
        Cluster::start(3, "rmdd27-kg-delegate-public-missing-authority")
            .await
            .expect("cluster starts"),
    );
    ensure_commons_graph(&cluster).await;
    let node_id = cluster
        .all_ids()
        .into_iter()
        .next()
        .expect("cluster has a node");
    let state = state_for(&cluster, node_id);
    let raft = state
        .read()
        .await
        .raft
        .clone()
        .expect("clustered fixture has a configured Raft handle");
    state
        .write()
        .await
        .install_missing_placement_authority(raft);

    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .expect("cluster member has persistence");
    let graph_fname = crate::persist::sanitize(GRAPH);
    let before_version = backend
        .read_mutation_graph_version(&graph_fname)
        .await
        .expect("read graph version before rejected admission")
        .unwrap_or(0);
    let response = dispatch_method(&cluster, node_id, 1, missing_authority_delegate_method()).await;
    assert_eq!(
        response.error.as_deref(),
        Some("CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required"),
        "missing clustered placement must refuse the signed KgDelegate: {response:?}"
    );
    assert!(
        !cluster
            .has_node_in(node_id, GRAPH, "rmdd27-missing-authority-work-item")
            .await,
        "missing clustered placement must not write a WorkItem"
    );
    let batch_id = missing_authority_delegate_batch_id();
    assert!(
        backend
            .read_mutation_batch(&graph_fname, &batch_id)
            .await
            .expect("read rejected admission receipt")
            .is_none(),
        "missing clustered placement must not write a MutationBatch receipt"
    );
    assert!(
        backend
            .read_mutation_outbox(&graph_fname, &batch_id)
            .await
            .expect("read rejected admission outbox")
            .is_empty(),
        "missing clustered placement must not write an outbox row"
    );
    let after_version = backend
        .read_mutation_graph_version(&graph_fname)
        .await
        .expect("read graph version after rejected admission")
        .unwrap_or(0);
    assert_eq!(after_version, before_version);

    cluster.finish().await;
}
