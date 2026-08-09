//! Adversarial unit coverage for the native resource authority helpers.
//!
//! These tests intentionally stay below the full server/Raft harness: they exercise
//! the exact predicates used by the one redb transaction, so the race and restart
//! integration suites can layer on top without duplicating policy logic.

use super::*;
use crate::mutation_batch::MutationRequestContext;

fn host() -> DurableResourceHost {
    DurableResourceHost {
        tenant_ref: "tenant-a".to_string(),
        host_ref: "host-1".to_string(),
        revision: 7,
        capacity: ResourceCapacity {
            cpu_weight: 8,
            memory_mib: 8_192,
            disk_mib: 10_000,
            process_slots: 4,
        },
        observed: ResourceCapacity {
            cpu_weight: 3,
            memory_mib: 1_024,
            disk_mib: 100,
            process_slots: 1,
        },
        heartbeat_at_ms: 1_000,
        heartbeat_ttl_ms: 120_000,
        now_ms: 1_000,
        draining: false,
        quarantined: false,
        labels: vec!["linux".to_string(), "rust".to_string()],
        target_kind: "local".to_string(),
        target_alias: None,
        disk_used_mib: 100,
        disk_capacity_mib: 10_000,
        held_cpu_weight: 2,
        held_memory_mib: 2_048,
        held_disk_mib: 200,
        held_process_slots: 1,
    }
}

fn request() -> ResourceReservationRequest {
    ResourceReservationRequest {
        schema_version: crate::epistemic_operations::ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: "tenant-a".to_string(),
        work_item_id: "work-1".to_string(),
        owner_id: "worker-a".to_string(),
        fence: "1".to_string(),
        lease_epoch: 1,
        fencing_token: 1,
        attempt: 1,
        reservation_id: "reservation-1".to_string(),
        input_fingerprint: format!("v1:{}", "0".repeat(64)),
        profile_name: "rust-build".to_string(),
        profile_version: "1".to_string(),
        host_ref: "host-1".to_string(),
        requirement: ResourceRequirement {
            cpu_weight: 2,
            memory_mib: 1_024,
            disk_mib: 200,
            process_slots: 1,
        },
        target_kind: ResourceReservationRequestTargetKind::Local,
        target_alias: None,
        repository_id: "repo".to_string(),
        branch: "main".to_string(),
        concurrency_key: "rust-build".to_string(),
        concurrency_limit: Some(1),
        repository_exclusive: false,
        branch_exclusive: false,
        required_labels: vec!["linux".to_string()],
        anti_affinity: vec!["compiler".to_string()],
        fairness_group: "default".to_string(),
        fairness_cost: 1,
        disk_low_watermark_mib: Some(500),
        disk_high_watermark_mib: Some(800),
        disk_policy_key: "rust-v1".to_string(),
        reserved_at_ms: 1_000,
        expires_at_ms: 61_000,
        idempotency_key: "reserve-invocation-1".to_string(),
        now_ms: 1_000,
        expected_host_revision: Some(7),
        expected_lifecycle_revision: None,
    }
}

fn work_item_props() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "node_type": "WorkItem",
        "tenant": "tenant-a",
        "status": "running",
        "lease_owner": "worker-a",
        "last_lease_owner": "worker-a",
        "attempt": 1,
        "lease_epoch": 1,
        "fencing_token": 1,
        "lease_expires_at": 61.0,
        "metadata": {
            "repository_work_item": {
                "contract_version": "1",
                "immutable_input_digest": format!("{}", "1".repeat(64)),
                "tenant_id": resource_b64_urlsafe("tenant-a"),
                "repository_id": resource_b64_urlsafe("repo"),
                "owner_id": resource_b64_urlsafe("worker-a"),
                "branch": resource_b64_urlsafe("main"),
                "job_id": resource_b64_urlsafe("job-1"),
                "target_kind": "local",
                "target_alias": null,
                "priority": 0,
                "queue_deadline": null,
                "resource_reservation": {
                    "schema_version": "1",
                    "resolved_profile_authority": "repository_manager:resource_profile_registry:v1",
                    "profile_name": resource_b64_urlsafe("rust-build"),
                    "profile_version": resource_b64_urlsafe("1"),
                    "cpu_weight": 2,
                    "memory_mib": 1_024,
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
                    "repository_id": resource_b64_urlsafe("repo"),
                    "concurrency_key": resource_b64_urlsafe("rust-build"),
                    "concurrency_limit": 1,
                    "repository_exclusive": false,
                    "branch_exclusive": false,
                    "fairness_group": resource_b64_urlsafe("default"),
                    "fairness_cost": 1,
                    "disk_policy_key": resource_b64_urlsafe("rust-v1"),
                    "disk_low_watermark_mib": 500,
                    "disk_high_watermark_mib": 800,
                    "branch": resource_b64_urlsafe("main"),
                    "branch_explicit": true,
                    "base_ref": resource_b64_urlsafe("main"),
                    "target_kind": "local",
                    "target_alias": null,
                    "work_item_input_fingerprint": format!("v1:{}", "1".repeat(64)),
                },
            }
        }
    })
    .as_object()
    .expect("object")
    .clone()
}

fn work_item_props_for_request(
    request: &ResourceReservationRequest,
) -> serde_json::Map<String, serde_json::Value> {
    let mut props = work_item_props();
    props.insert("tenant".to_string(), serde_json::json!(request.tenant_ref));
    props.insert("status".to_string(), serde_json::json!("running"));
    props.insert(
        "lease_owner".to_string(),
        serde_json::json!(request.owner_id),
    );
    props.insert(
        "last_lease_owner".to_string(),
        serde_json::json!(request.owner_id),
    );
    props.insert("attempt".to_string(), serde_json::json!(request.attempt));
    props.insert(
        "lease_epoch".to_string(),
        serde_json::json!(request.lease_epoch),
    );
    props.insert(
        "fencing_token".to_string(),
        serde_json::json!(request.fencing_token),
    );
    props.insert(
        "lease_expires_at".to_string(),
        serde_json::json!(request.expires_at_ms as f64 / 1_000.0),
    );
    let repository = props
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|metadata| metadata.get_mut("repository_work_item"))
        .and_then(serde_json::Value::as_object_mut)
        .expect("repository WorkItem metadata");
    repository.insert(
        "tenant_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.tenant_ref)),
    );
    repository.insert(
        "repository_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.repository_id)),
    );
    repository.insert(
        "owner_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.owner_id)),
    );
    repository.insert(
        "branch".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.branch)),
    );
    repository.insert(
        "target_kind".to_string(),
        serde_json::Value::String(resource_request_target_kind(request.target_kind).to_string()),
    );
    repository.insert(
        "target_alias".to_string(),
        request
            .target_alias
            .as_deref()
            .map_or(serde_json::Value::Null, |alias| {
                serde_json::Value::String(resource_b64_urlsafe(alias))
            }),
    );
    let extension = repository
        .get_mut("resource_reservation")
        .and_then(serde_json::Value::as_object_mut)
        .expect("resource reservation extension");
    extension.insert(
        "profile_name".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.profile_name)),
    );
    extension.insert(
        "profile_version".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.profile_version)),
    );
    extension.insert(
        "cpu_weight".to_string(),
        serde_json::json!(request.requirement.cpu_weight),
    );
    extension.insert(
        "memory_mib".to_string(),
        serde_json::json!(request.requirement.memory_mib),
    );
    extension.insert(
        "disk_mib".to_string(),
        serde_json::json!(request.requirement.disk_mib),
    );
    extension.insert(
        "process_slots".to_string(),
        serde_json::json!(request.requirement.process_slots),
    );
    extension.insert(
        "host_labels".to_string(),
        serde_json::Value::Array(
            request
                .required_labels
                .iter()
                .map(|label| serde_json::Value::String(resource_b64_urlsafe(label)))
                .collect(),
        ),
    );
    extension.insert(
        "anti_affinity".to_string(),
        serde_json::Value::Array(
            request
                .anti_affinity
                .iter()
                .map(|tag| serde_json::Value::String(resource_b64_urlsafe(tag)))
                .collect(),
        ),
    );
    extension.insert(
        "repository_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.repository_id)),
    );
    extension.insert(
        "concurrency_key".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.concurrency_key)),
    );
    extension.insert(
        "concurrency_limit".to_string(),
        request
            .concurrency_limit
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    extension.insert(
        "repository_exclusive".to_string(),
        serde_json::json!(request.repository_exclusive),
    );
    extension.insert(
        "branch_exclusive".to_string(),
        serde_json::json!(request.branch_exclusive),
    );
    extension.insert(
        "fairness_group".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.fairness_group)),
    );
    extension.insert(
        "fairness_cost".to_string(),
        serde_json::json!(request.fairness_cost),
    );
    extension.insert(
        "disk_policy_key".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.disk_policy_key)),
    );
    extension.insert(
        "disk_low_watermark_mib".to_string(),
        request
            .disk_low_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    extension.insert(
        "disk_high_watermark_mib".to_string(),
        request
            .disk_high_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    extension.insert(
        "branch".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.branch)),
    );
    extension.insert("branch_explicit".to_string(), serde_json::json!(true));
    extension.insert(
        "base_ref".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.branch)),
    );
    extension.insert(
        "target_kind".to_string(),
        serde_json::Value::String(resource_request_target_kind(request.target_kind).to_string()),
    );
    extension.insert(
        "target_alias".to_string(),
        request
            .target_alias
            .as_deref()
            .map_or(serde_json::Value::Null, |alias| {
                serde_json::Value::String(resource_b64_urlsafe(alias))
            }),
    );
    props
}

fn seed_resource_database(
    path: &std::path::Path,
    work_items: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    hosts: Vec<DurableResourceHost>,
) -> Database {
    let db = Database::create(path).expect("create resource race database");
    let wtx = db.begin_write().expect("begin resource race seed");
    initialize_canonical_tables(&wtx).expect("initialize resource tables");
    {
        let mut nodes = wtx.open_table(NODES).unwrap();
        for (id, props) in work_items {
            let bytes = rmp_serde::to_vec_named(&props).unwrap();
            nodes
                .insert(("graph-a", id.as_str()), bytes.as_slice())
                .unwrap();
        }
        let mut host_table = wtx.open_table(RESOURCE_HOSTS).unwrap();
        for host in &hosts {
            resource_put_host(&mut host_table, "graph-a", host, DurableCrypto::none()).unwrap();
        }
    }
    wtx.commit().expect("commit resource race seed");
    db
}

fn resource_batch(
    tenant: &str,
    method: Method,
    batch_id: &str,
    idempotency_key: &str,
) -> MutationBatch {
    MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        context: MutationRequestContext {
            request_id: 77,
            principal: format!("principal:sha256:{}", "b".repeat(64)),
            purpose: Some("resource-transaction-test".to_string()),
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: tenant.to_string(),
        graph: "graph-a".to_string(),
        placement_epoch: 0,
        idempotency_key: idempotency_key.to_string(),
        expected_graph_version: None,
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: MutationDomain::ControlPlane,
            method,
        }],
        outbox: Vec::new(),
        created_at_ms: 1_000,
    }
}

fn commit_resource_batch_at(
    db: &Database,
    batch: &MutationBatch,
    crashpoint: Option<MutationBatchCrashpoint>,
) -> Result<MutationBatchCommit, String> {
    #[cfg(feature = "security")]
    let mut audit = AuditTailCache::new();
    commit_mutation_batch_inner(
        db,
        "graph-a",
        batch,
        None,
        None,
        None,
        None,
        1_000,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut audit,
        true,
        crashpoint,
    )
}

fn commit_resource_batch(
    db: &Database,
    batch: &MutationBatch,
) -> Result<MutationBatchCommit, String> {
    commit_resource_batch_at(db, batch, None)
}

fn batch_resource_result(commit: &MutationBatchCommit) -> ResourceReservationResult {
    let bytes = commit
        .record
        .result_msgpack
        .as_ref()
        .expect("resource mutation stores a typed result");
    let payload: crate::protocol::ResultPayload = rmp_serde::from_slice(bytes).unwrap();
    resource_decode_result_payload(payload).unwrap()
}

fn resolved_request() -> (
    ResourceReservationRequest,
    serde_json::Map<String, serde_json::Value>,
) {
    let mut request = request();
    let props = work_item_props_for_request(&request);
    request.input_fingerprint = resource_recomputed_fingerprint(&props, &request)
        .expect("test WorkItem has a complete resolved projection");
    (request, props)
}

fn batch_host_result(commit: &MutationBatchCommit) -> ResourceHostUpdateResult {
    let bytes = commit
        .record
        .result_msgpack
        .as_ref()
        .expect("host mutation stores a typed result");
    let payload: crate::protocol::ResultPayload = rmp_serde::from_slice(bytes).unwrap();
    let (crate::protocol::ResultPayload::Raw(bytes)
    | crate::protocol::ResultPayload::PropertiesMsgpack(bytes)) = payload
    else {
        panic!("host mutation result must be raw typed payload");
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode host mutation result")
}

fn single_reserve_decision(
    suffix: &str,
    request: ResourceReservationRequest,
    props: serde_json::Map<String, serde_json::Value>,
    host: DurableResourceHost,
    concurrency_count: Option<u64>,
    anti_affinity_count: Option<(&str, u64)>,
) -> ResourceReservationResult {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-policy-{suffix}-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let db = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host],
    );
    if concurrency_count.is_some() || anti_affinity_count.is_some() {
        let wtx = db.begin_write().expect("begin policy counter seed");
        if let Some(count) = concurrency_count {
            let mut concurrency = wtx.open_table(RESOURCE_CONCURRENCY).unwrap();
            let key = resource_concurrency_scope_key(&request.concurrency_key);
            concurrency
                .insert(("graph-a", key.as_str()), count)
                .unwrap();
            drop(concurrency);
        }
        if let Some((tag, count)) = anti_affinity_count {
            let mut anti_affinity = wtx.open_table(RESOURCE_ANTI_AFFINITY).unwrap();
            anti_affinity
                .insert(("graph-a", request.host_ref.as_str(), tag), count)
                .unwrap();
            drop(anti_affinity);
        }
        wtx.commit().expect("commit policy counter seed");
    }
    let tenant_ref = request.tenant_ref.clone();
    let batch = resource_batch(
        &tenant_ref,
        Method::ReserveWorkItemResources { request },
        &format!("batch-policy-{suffix}"),
        &format!("reserve-policy-{suffix}"),
    );
    let result = batch_resource_result(&commit_resource_batch(&db, &batch).expect("policy result"));
    drop(db);
    let _ = std::fs::remove_file(path);
    result
}

#[test]
fn capacity_accounts_for_live_observed_usage_and_checked_last_slot() {
    let host = host();
    let requirement = ResourceRequirement {
        cpu_weight: 3,
        memory_mib: 5_120,
        disk_mib: 9_701,
        process_slots: 2,
    };
    assert!(!resource_capacity_sum(&host, &requirement));
    let one_slot = ResourceRequirement {
        cpu_weight: 3,
        memory_mib: 5_120,
        disk_mib: 9_700,
        process_slots: 1,
    };
    assert!(resource_capacity_sum(&host, &one_slot));
    let overflow = ResourceRequirement {
        cpu_weight: u64::MAX,
        ..one_slot
    };
    assert!(!resource_capacity_sum(&host, &overflow));
}

#[test]
fn native_transaction_policy_refusals_cover_host_and_accounting_guards() {
    let (base, _) = resolved_request();

    let mut labels_request = base.clone();
    labels_request.required_labels = vec!["windows".to_string()];
    let labels_props = work_item_props_for_request(&labels_request);
    labels_request.input_fingerprint =
        resource_recomputed_fingerprint(&labels_props, &labels_request)
            .expect("labels WorkItem projection");
    let labels =
        single_reserve_decision("labels", labels_request, labels_props, host(), None, None);
    assert_eq!(labels.decision, ResourceReservationResultDecision::Labels);
    assert_eq!(labels.held_cpu_weight, 0);

    let mut quarantined_host = host();
    quarantined_host.quarantined = true;
    let quarantined = single_reserve_decision(
        "quarantine",
        base.clone(),
        work_item_props_for_request(&base),
        quarantined_host,
        None,
        None,
    );
    assert_eq!(
        quarantined.decision,
        ResourceReservationResultDecision::Quarantined
    );

    let mut future_heartbeat_host = host();
    future_heartbeat_host.heartbeat_at_ms = 1_001;
    let future_heartbeat = single_reserve_decision(
        "future-heartbeat",
        base.clone(),
        work_item_props_for_request(&base),
        future_heartbeat_host,
        None,
        None,
    );
    assert_eq!(
        future_heartbeat.decision,
        ResourceReservationResultDecision::StaleHost
    );

    let mut stale_request = base.clone();
    stale_request.now_ms = 2_001;
    let mut stale_host = host();
    stale_host.heartbeat_at_ms = 0;
    stale_host.heartbeat_ttl_ms = 1_000;
    let stale_heartbeat = single_reserve_decision(
        "stale-heartbeat",
        stale_request.clone(),
        work_item_props_for_request(&stale_request),
        stale_host,
        None,
        None,
    );
    assert_eq!(
        stale_heartbeat.decision,
        ResourceReservationResultDecision::StaleHost
    );

    let anti_affinity = single_reserve_decision(
        "anti-affinity",
        base.clone(),
        work_item_props_for_request(&base),
        host(),
        None,
        Some(("compiler", 1)),
    );
    assert_eq!(
        anti_affinity.decision,
        ResourceReservationResultDecision::AntiAffinity
    );

    let concurrency = single_reserve_decision(
        "concurrency",
        base.clone(),
        work_item_props_for_request(&base),
        host(),
        Some(1),
        None,
    );
    assert_eq!(
        concurrency.decision,
        ResourceReservationResultDecision::Concurrency
    );

    let mut observed_host = host();
    observed_host.observed.cpu_weight = observed_host.capacity.cpu_weight;
    let observed = single_reserve_decision(
        "observed-capacity",
        base.clone(),
        work_item_props_for_request(&base),
        observed_host,
        None,
        None,
    );
    assert_eq!(
        observed.decision,
        ResourceReservationResultDecision::Capacity
    );

    let mut remote_host = host();
    remote_host.target_kind = "inventory_alias".to_string();
    remote_host.target_alias = Some("remote-a".to_string());
    let target = single_reserve_decision(
        "target-mismatch",
        base.clone(),
        work_item_props_for_request(&base),
        remote_host,
        None,
        None,
    );
    assert_eq!(target.decision, ResourceReservationResultDecision::Policy);

    // A refusal is a decision only; it must never mutate scheduler debt or
    // expose held accounting.  Keep this assertion over every refusal in the
    // matrix so a newly added policy guard cannot accidentally charge debt.
    for refusal in [
        &labels,
        &quarantined,
        &future_heartbeat,
        &stale_heartbeat,
        &anti_affinity,
        &concurrency,
        &observed,
        &target,
    ] {
        assert_eq!(refusal.fairness_debt, 0, "refusal must not accrue debt");
        assert_eq!(refusal.held_cpu_weight, 0, "refusal must not hold CPU");
        assert_eq!(refusal.held_memory_mib, 0, "refusal must hold no memory");
        assert_eq!(refusal.held_disk_mib, 0, "refusal must hold no disk");
        assert_eq!(refusal.held_process_slots, 0, "refusal must hold no slots");
    }
}

#[test]
fn mutation_batch_same_attempt_race_has_one_durable_winner_and_replay() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-batch-race-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let db = std::sync::Arc::new(seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    ));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let batch_a = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-same-attempt-a",
        "reserve-same-attempt-a",
    );
    let mut invocation_b = request.clone();
    invocation_b.idempotency_key = "reserve-same-attempt-b".to_string();
    let batch_b = resource_batch(
        &invocation_b.tenant_ref.clone(),
        Method::ReserveWorkItemResources {
            request: invocation_b,
        },
        "batch-same-attempt-b",
        "reserve-same-attempt-b",
    );
    let mut handles = Vec::new();
    for batch in [batch_a.clone(), batch_b] {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            commit_resource_batch(&db, &batch)
        }));
    }
    let commits: Vec<_> = handles
        .into_iter()
        .map(|handle| {
            handle
                .join()
                .expect("resource race worker")
                .expect("same-attempt race commit")
        })
        .collect();
    assert!(commits.iter().all(|commit| !commit.replayed));
    let decisions: Vec<_> = commits
        .iter()
        .map(|commit| batch_resource_result(commit).decision)
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Idempotent)
            .count(),
        1
    );
    let batch_a_decision = commits
        .iter()
        .find(|commit| commit.record.batch.batch_id == "batch-same-attempt-a")
        .map(batch_resource_result)
        .expect("batch A result");
    let replay = commit_resource_batch(&db, &batch_a).expect("transport replay after race");
    assert!(replay.replayed);
    assert_eq!(
        batch_resource_result(&replay).decision,
        batch_a_decision.decision
    );
    drop(db);
    let reopened = Database::open(&path).expect("reopen race database");
    let wtx = reopened.begin_write().expect("inspect race authority");
    let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
    let stored_host = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
        .unwrap()
        .expect("host survives race");
    assert_eq!(
        stored_host.held_cpu_weight,
        host().held_cpu_weight + request.requirement.cpu_weight
    );
    assert_eq!(
        stored_host.held_process_slots,
        host().held_process_slots + request.requirement.process_slots
    );
    drop(hosts);
    wtx.commit().expect("commit race inspection");
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_distinct_reservation_id_same_attempt_refuses_without_recharge() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-distinct-reservation-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let db = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    let first = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-distinct-reservation-first",
        "reserve-distinct-reservation-first",
    );
    let accepted = commit_resource_batch(&db, &first).expect("first same-attempt reserve");
    assert_eq!(
        batch_resource_result(&accepted).decision,
        ResourceReservationResultDecision::Accepted
    );

    let expected_cpu_weight = request.requirement.cpu_weight;
    let mut conflicting_request = request;
    conflicting_request.reservation_id = "reservation-other".to_string();
    conflicting_request.idempotency_key = "reserve-distinct-reservation-other".to_string();
    conflicting_request.input_fingerprint = resource_recomputed_fingerprint(
        &work_item_props_for_request(&conflicting_request),
        &conflicting_request,
    )
    .expect("conflicting caller supplies a self-consistent request fingerprint");
    let conflicting_tenant_ref = conflicting_request.tenant_ref.clone();
    let conflicting = resource_batch(
        &conflicting_tenant_ref,
        Method::ReserveWorkItemResources {
            request: conflicting_request,
        },
        "batch-distinct-reservation-other",
        "reserve-distinct-reservation-other",
    );
    let refused = commit_resource_batch(&db, &conflicting).expect("distinct reservation result");
    assert_eq!(
        batch_resource_result(&refused).decision,
        ResourceReservationResultDecision::Conflict
    );
    let wtx = db
        .begin_write()
        .expect("inspect distinct reservation authority");
    let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
    let stored_host = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
        .unwrap()
        .expect("host survives distinct reservation refusal");
    assert_eq!(
        stored_host.held_cpu_weight,
        host().held_cpu_weight + expected_cpu_weight
    );
    drop(hosts);
    wtx.commit()
        .expect("commit distinct reservation inspection");
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_distinct_work_items_race_for_last_slot() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-last-slot-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (mut request_a, _) = resolved_request();
    request_a.concurrency_limit = None;
    request_a.anti_affinity.clear();
    let props_a = work_item_props_for_request(&request_a);
    request_a.input_fingerprint = resource_recomputed_fingerprint(&props_a, &request_a)
        .expect("last-slot WorkItem has a complete resolved projection");
    let mut request_b = request_a.clone();
    request_b.work_item_id = "work-2".to_string();
    request_b.owner_id = "worker-b".to_string();
    request_b.fence = "2".to_string();
    request_b.lease_epoch = 2;
    request_b.fencing_token = 2;
    request_b.reservation_id = "reservation-2".to_string();
    request_b.idempotency_key = "reserve-last-slot-b".to_string();
    let props_b = work_item_props_for_request(&request_b);
    request_b.input_fingerprint = resource_recomputed_fingerprint(&props_b, &request_b)
        .expect("second WorkItem has a complete resolved projection");
    let mut constrained_host = host();
    constrained_host.capacity.process_slots =
        constrained_host.observed.process_slots + constrained_host.held_process_slots + 1;
    let db = std::sync::Arc::new(seed_resource_database(
        &path,
        vec![
            (request_a.work_item_id.clone(), props_a),
            (request_b.work_item_id.clone(), props_b),
        ],
        vec![constrained_host.clone()],
    ));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let batch_a = resource_batch(
        &request_a.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request_a.clone(),
        },
        "batch-last-slot-a",
        "reserve-last-slot-a",
    );
    let batch_b = resource_batch(
        &request_b.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request_b.clone(),
        },
        "batch-last-slot-b",
        "reserve-last-slot-b",
    );
    let handles = [batch_a, batch_b]
        .into_iter()
        .map(|batch| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let commit = commit_resource_batch(&db, &batch).expect("last-slot commit");
                batch_resource_result(&commit).decision
            })
        })
        .collect::<Vec<_>>();
    let decisions: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("last-slot worker"))
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Capacity)
            .count(),
        1
    );
    drop(db);
    let reopened = Database::open(&path).expect("reopen last-slot database");
    let wtx = reopened.begin_write().expect("inspect last-slot authority");
    let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
    let stored_host = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
        .unwrap()
        .expect("last-slot host survives");
    assert_eq!(
        stored_host.held_process_slots,
        constrained_host.held_process_slots + 1
    );
    drop(hosts);
    wtx.commit().expect("commit last-slot inspection");
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_transient_refusal_needs_fresh_invocation_but_acceptance_replays() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-fresh-invocation-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let mut draining = host();
    draining.draining = true;
    let db = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![draining.clone()],
    );
    let refused_batch = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-transient-refusal",
        "reserve-transient-refusal",
    );
    let refused = commit_resource_batch(&db, &refused_batch).expect("persist transient refusal");
    assert!(!refused.replayed);
    assert_eq!(
        batch_resource_result(&refused).decision,
        ResourceReservationResultDecision::Drained
    );
    {
        let wtx = db.begin_write().expect("inspect refused fairness state");
        let mut fairness = wtx.open_table(RESOURCE_FAIRNESS).unwrap();
        assert_eq!(
            resource_load_fairness(
                &mut fairness,
                "graph-a",
                &request.tenant_ref,
                &request.fairness_group,
                DurableCrypto::none(),
            )
            .unwrap()
            .debt,
            0,
            "refused admission must not accrue fairness debt"
        );
        drop(fairness);
        wtx.commit().expect("commit refused fairness inspection");
    }

    let host_update = ResourceHostUpdateRequest {
        schema_version: crate::epistemic_operations::ResourceHostUpdateRequestSchemaVersion::V1,
        tenant_ref: draining.tenant_ref.clone(),
        host_ref: draining.host_ref.clone(),
        revision: draining.revision + 1,
        capacity: draining.capacity.clone(),
        observed: draining.observed.clone(),
        heartbeat_at_ms: 2_000,
        heartbeat_ttl_ms: draining.heartbeat_ttl_ms,
        now_ms: 2_000,
        draining: false,
        quarantined: false,
        labels: draining.labels.clone(),
        target_kind: ResourceHostUpdateRequestTargetKind::Local,
        target_alias: None,
        disk_used_mib: draining.disk_used_mib,
        disk_capacity_mib: draining.disk_capacity_mib,
    };
    let update_batch = resource_batch(
        &request.tenant_ref,
        Method::UpdateResourceHost {
            request: host_update,
        },
        "batch-clear-drain",
        "host-clear-drain",
    );
    let update = commit_resource_batch(&db, &update_batch).expect("clear host drain");
    assert!(!update.replayed);
    assert_eq!(
        batch_host_result(&update).reason,
        ResourceHostUpdateResultReason::Accepted
    );
    let refused_replay = commit_resource_batch(&db, &refused_batch).expect("replay refusal");
    assert!(refused_replay.replayed);
    assert_eq!(
        batch_resource_result(&refused_replay).decision,
        ResourceReservationResultDecision::Drained
    );

    let mut fresh_request = request.clone();
    fresh_request.idempotency_key = "reserve-fresh-after-drain".to_string();
    fresh_request.now_ms = 2_000;
    fresh_request.expected_host_revision = Some(draining.revision + 1);
    let fresh_batch = resource_batch(
        &fresh_request.tenant_ref.clone(),
        Method::ReserveWorkItemResources {
            request: fresh_request,
        },
        "batch-fresh-after-drain",
        "reserve-fresh-after-drain",
    );
    let accepted = commit_resource_batch(&db, &fresh_batch).expect("fresh reserve invocation");
    assert!(!accepted.replayed);
    assert_eq!(
        batch_resource_result(&accepted).decision,
        ResourceReservationResultDecision::Accepted
    );
    let replay = commit_resource_batch(&db, &fresh_batch).expect("exact accepted replay");
    assert!(replay.replayed);
    assert_eq!(
        batch_resource_result(&replay).decision,
        ResourceReservationResultDecision::Accepted
    );
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_cross_host_repository_and_branch_exclusivity_is_atomic() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-cross-host-exclusive-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (mut request_a, _) = resolved_request();
    request_a.concurrency_limit = None;
    request_a.repository_exclusive = true;
    request_a.branch_exclusive = true;
    let props_a = work_item_props_for_request(&request_a);
    request_a.input_fingerprint = resource_recomputed_fingerprint(&props_a, &request_a)
        .expect("exclusive WorkItem has a complete resolved projection");
    let mut request_b = request_a.clone();
    request_b.work_item_id = "work-2".to_string();
    request_b.owner_id = "worker-b".to_string();
    request_b.fence = "2".to_string();
    request_b.lease_epoch = 2;
    request_b.fencing_token = 2;
    request_b.reservation_id = "reservation-2".to_string();
    request_b.host_ref = "host-2".to_string();
    request_b.idempotency_key = "reserve-exclusive-b".to_string();
    let props_b = work_item_props_for_request(&request_b);
    request_b.input_fingerprint = resource_recomputed_fingerprint(&props_b, &request_b)
        .expect("second exclusive WorkItem has a complete resolved projection");
    let host_two = {
        let mut value = host();
        value.host_ref = "host-2".to_string();
        value
    };
    let db = seed_resource_database(
        &path,
        vec![
            (request_a.work_item_id.clone(), props_a),
            (request_b.work_item_id.clone(), props_b),
        ],
        vec![host(), host_two.clone()],
    );
    let batch_a = resource_batch(
        &request_a.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request_a.clone(),
        },
        "batch-exclusive-a",
        "reserve-exclusive-a",
    );
    let batch_b = resource_batch(
        &request_b.tenant_ref.clone(),
        Method::ReserveWorkItemResources { request: request_b },
        "batch-exclusive-b",
        "reserve-exclusive-b",
    );
    let db = std::sync::Arc::new(db);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = [batch_a, batch_b]
        .into_iter()
        .map(|batch| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let commit = commit_resource_batch(&db, &batch).expect("exclusive race commit");
                batch_resource_result(&commit).decision
            })
        })
        .collect::<Vec<_>>();
    let decisions: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("exclusive race worker"))
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Exclusivity)
            .count(),
        1
    );
    let wtx = db.begin_write().expect("inspect cross-host exclusivity");
    let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
    let host_one = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
        .unwrap()
        .expect("first host remains present");
    let host_two_after = resource_load_host(&mut hosts, "graph-a", "host-2", DurableCrypto::none())
        .unwrap()
        .expect("second host remains present");
    let first_delta = host_one.held_cpu_weight - host().held_cpu_weight;
    let second_delta = host_two_after.held_cpu_weight - host_two.held_cpu_weight;
    assert_eq!(
        [first_delta, second_delta]
            .into_iter()
            .filter(|delta| *delta == request_a.requirement.cpu_weight)
            .count(),
        1,
        "exactly one host wins the exclusivity race"
    );
    assert_eq!(
        first_delta + second_delta,
        request_a.requirement.cpu_weight,
        "exclusive refusal cannot charge both hosts"
    );
    drop(hosts);
    wtx.commit().expect("commit exclusivity inspection");
    drop(db);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_release_and_superseded_reclaim_replay_tombstones_after_reopen() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-lifecycle-batch-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let db = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    let reserve_batch = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-lifecycle-reserve",
        "reserve-lifecycle",
    );
    let reserved = commit_resource_batch(&db, &reserve_batch).expect("reserve lifecycle hold");
    assert_eq!(
        batch_resource_result(&reserved).decision,
        ResourceReservationResultDecision::Accepted
    );

    let mut release_request = request.clone();
    release_request.expected_lifecycle_revision = Some(1);
    release_request.now_ms = 2_000;
    release_request.idempotency_key = "release-lifecycle".to_string();
    let release_batch = resource_batch(
        &release_request.tenant_ref,
        Method::ReleaseWorkItemResources {
            request: release_request.clone(),
        },
        "batch-lifecycle-release",
        "release-lifecycle",
    );
    let released = commit_resource_batch(&db, &release_batch).expect("release lifecycle hold");
    let released_result = batch_resource_result(&released);
    assert_eq!(
        released_result.decision,
        ResourceReservationResultDecision::Accepted
    );
    assert_eq!(
        released_result.state,
        ResourceReservationResultState::Released
    );
    assert_eq!(released_result.held_cpu_weight, 0);
    assert_eq!(released_result.held_memory_mib, 0);
    assert_eq!(released_result.held_disk_mib, 0);
    assert_eq!(released_result.held_process_slots, 0);
    let replay_release = commit_resource_batch(&db, &release_batch).expect("replay release");
    assert!(replay_release.replayed);
    assert_eq!(
        batch_resource_result(&replay_release).decision,
        ResourceReservationResultDecision::Accepted
    );
    let mut stale_release = release_request.clone();
    stale_release.expected_lifecycle_revision = Some(2);
    let stale_release_batch = resource_batch(
        &stale_release.tenant_ref.clone(),
        Method::ReleaseWorkItemResources {
            request: stale_release,
        },
        "batch-lifecycle-release-stale",
        "release-lifecycle-stale",
    );
    let stale = commit_resource_batch(&db, &stale_release_batch).expect("stale release result");
    assert_eq!(
        batch_resource_result(&stale).decision,
        ResourceReservationResultDecision::InputConflict
    );

    let (mut reclaim_request, _) = resolved_request();
    reclaim_request.work_item_id = "work-2".to_string();
    reclaim_request.owner_id = "worker-b".to_string();
    reclaim_request.fence = "2".to_string();
    reclaim_request.lease_epoch = 2;
    reclaim_request.fencing_token = 2;
    reclaim_request.reservation_id = "reservation-2".to_string();
    reclaim_request.idempotency_key = "reserve-reclaim".to_string();
    let reclaim_props = work_item_props_for_request(&reclaim_request);
    reclaim_request.input_fingerprint =
        resource_recomputed_fingerprint(&reclaim_props, &reclaim_request)
            .expect("reclaim WorkItem has a complete resolved projection");
    let reclaim_reserve = resource_batch(
        &reclaim_request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: reclaim_request.clone(),
        },
        "batch-reclaim-reserve",
        "reserve-reclaim",
    );
    let reserve_two = {
        let props = work_item_props_for_request(&reclaim_request);
        let wtx = db.begin_write().expect("begin second WorkItem seed");
        let mut nodes = wtx.open_table(NODES).unwrap();
        let bytes = rmp_serde::to_vec_named(&props).unwrap();
        nodes
            .insert(
                ("graph-a", reclaim_request.work_item_id.as_str()),
                bytes.as_slice(),
            )
            .unwrap();
        drop(nodes);
        wtx.commit().expect("commit second WorkItem seed");
        commit_resource_batch(&db, &reclaim_reserve).expect("reserve reclaim hold")
    };
    assert_eq!(
        batch_resource_result(&reserve_two).decision,
        ResourceReservationResultDecision::Accepted
    );
    {
        let wtx = db.begin_write().expect("advance WorkItem attempt");
        let mut nodes = wtx.open_table(NODES).unwrap();
        let current = nodes
            .get(("graph-a", reclaim_request.work_item_id.as_str()))
            .unwrap()
            .map(|value| {
                decode_durable::<serde_json::Map<String, serde_json::Value>>(value.value())
            })
            .transpose()
            .unwrap()
            .expect("second WorkItem row");
        let mut current = current;
        current.insert("status".to_string(), serde_json::json!("ready"));
        current.insert("lease_owner".to_string(), serde_json::json!(""));
        current.insert(
            "last_lease_owner".to_string(),
            serde_json::json!("worker-b"),
        );
        current.insert("attempt".to_string(), serde_json::json!(2));
        current.insert("lease_epoch".to_string(), serde_json::json!(3));
        current.insert("fencing_token".to_string(), serde_json::json!(3));
        current.insert("lease_expires_at".to_string(), serde_json::json!(0.0));
        let bytes = rmp_serde::to_vec_named(&current).unwrap();
        nodes
            .insert(
                ("graph-a", reclaim_request.work_item_id.as_str()),
                bytes.as_slice(),
            )
            .unwrap();
        drop(nodes);
        wtx.commit().expect("commit superseded WorkItem attempt");
    }
    let mut reclaim = reclaim_request.clone();
    reclaim.expected_lifecycle_revision = Some(1);
    reclaim.now_ms = reclaim.expires_at_ms;
    reclaim.idempotency_key = "reclaim-lifecycle".to_string();
    let reclaim_batch = resource_batch(
        &reclaim.tenant_ref.clone(),
        Method::ReclaimWorkItemResources { request: reclaim },
        "batch-lifecycle-reclaim",
        "reclaim-lifecycle",
    );
    let reclaimed = commit_resource_batch(&db, &reclaim_batch).expect("reclaim superseded hold");
    let reclaimed_result = batch_resource_result(&reclaimed);
    assert_eq!(
        reclaimed_result.decision,
        ResourceReservationResultDecision::Accepted
    );
    assert_eq!(
        reclaimed_result.state,
        ResourceReservationResultState::Superseded
    );
    assert_eq!(reclaimed_result.held_cpu_weight, 0);
    assert_eq!(reclaimed_result.held_memory_mib, 0);
    assert_eq!(reclaimed_result.held_disk_mib, 0);
    assert_eq!(reclaimed_result.held_process_slots, 0);
    let replay_reclaim = commit_resource_batch(&db, &reclaim_batch).expect("replay reclaim");
    assert!(replay_reclaim.replayed);
    assert_eq!(
        batch_resource_result(&replay_reclaim).state,
        ResourceReservationResultState::Superseded
    );
    drop(db);

    let reopened = Database::open(&path).expect("reopen lifecycle database");
    let wtx = reopened
        .begin_write()
        .expect("inspect lifecycle tombstones");
    let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
    let released = resource_load_reservation(
        &mut reservations,
        "graph-a",
        "reservation-1",
        DurableCrypto::none(),
    )
    .unwrap()
    .expect("released tombstone survives restart");
    assert_eq!(
        released.record.state,
        ResourceReservationRecordState::Released
    );
    assert_eq!(released.held_cpu_weight, 0);
    assert_eq!(released.held_memory_mib, 0);
    assert_eq!(released.held_disk_mib, 0);
    assert_eq!(released.held_process_slots, 0);
    let superseded = resource_load_reservation(
        &mut reservations,
        "graph-a",
        "reservation-2",
        DurableCrypto::none(),
    )
    .unwrap()
    .expect("superseded tombstone survives restart");
    assert_eq!(
        superseded.record.state,
        ResourceReservationRecordState::Superseded
    );
    assert_eq!(superseded.held_cpu_weight, 0);
    assert_eq!(superseded.held_memory_mib, 0);
    assert_eq!(superseded.held_disk_mib, 0);
    assert_eq!(superseded.held_process_slots, 0);
    drop(reservations);
    wtx.commit().expect("commit lifecycle inspection");
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_resource_crashpoints_reopen_all_or_nothing_and_replay() {
    for (index, point) in [
        MutationBatchCrashpoint::BeforeRows,
        MutationBatchCrashpoint::AfterRowsBeforeMetadata,
        MutationBatchCrashpoint::BeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let path = std::env::temp_dir().join(format!(
            "eg-resource-crashpoint-{index}-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let (request, props) = resolved_request();
        let db = seed_resource_database(
            &path,
            vec![(request.work_item_id.clone(), props)],
            vec![host()],
        );
        let batch = resource_batch(
            &request.tenant_ref,
            Method::ReserveWorkItemResources {
                request: request.clone(),
            },
            &format!("batch-resource-crash-{index}"),
            &format!("reserve-resource-crash-{index}"),
        );
        assert!(commit_resource_batch_at(&db, &batch, Some(point)).is_err());
        drop(db);
        let reopened = Database::open(&path).expect("reopen precommit resource database");
        assert!(
            read_mutation_batch(&reopened, &batch.batch_id, DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        assert!(
            read_mutation_outbox(&reopened, &batch.batch_id, DurableCrypto::none())
                .expect("read precommit outbox")
                .is_empty(),
            "a rolled-back resource mutation must not leave an outbox row"
        );
        let wtx = reopened
            .begin_write()
            .expect("inspect precommit resource state");
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let current = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
            .unwrap()
            .expect("precommit host survives");
        assert_eq!(current.held_cpu_weight, host().held_cpu_weight);
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        assert!(resource_load_reservation(
            &mut reservations,
            "graph-a",
            &request.reservation_id,
            DurableCrypto::none(),
        )
        .unwrap()
        .is_none());
        drop(reservations);
        drop(hosts);
        wtx.commit().expect("commit precommit inspection");
        #[cfg(feature = "security")]
        {
            let audit = verify_audit(&reopened, "graph-a").expect("verify rollback audit");
            assert!(audit.ok);
            assert_eq!(
                audit.entries, 0,
                "a rolled-back resource mutation must not leave an audit row"
            );
        }
        let _ = std::fs::remove_file(path);
    }

    let path = std::env::temp_dir().join(format!(
        "eg-resource-postcommit-crash-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let db = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    let batch = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-resource-postcommit",
        "reserve-resource-postcommit",
    );
    assert!(commit_resource_batch_at(
        &db,
        &batch,
        Some(MutationBatchCrashpoint::AfterCommitBeforeAck),
    )
    .is_err());
    drop(db);
    let reopened = Database::open(&path).expect("reopen postcommit resource database");
    let before_outbox = read_mutation_outbox(&reopened, &batch.batch_id, DurableCrypto::none())
        .expect("read postcommit resource outbox");
    assert_eq!(before_outbox.len(), 1, "one canonical resource outbox row");
    assert_eq!(before_outbox[0].batch_id, batch.batch_id);
    assert_eq!(before_outbox[0].intent.topic, "engine.mutation.committed");
    assert_eq!(before_outbox[0].intent.key, batch.batch_id);
    let outbox_operation: MutationOperation =
        rmp_serde::from_slice(&before_outbox[0].intent.payload)
            .expect("decode durable resource outbox operation");
    assert!(matches!(
        outbox_operation.method,
        Method::ReserveWorkItemResources { .. }
    ));
    let replay = commit_resource_batch(&reopened, &batch).expect("postcommit resource replay");
    assert!(replay.replayed);
    assert_eq!(
        read_mutation_outbox(&reopened, &batch.batch_id, DurableCrypto::none())
            .expect("read replayed resource outbox"),
        before_outbox
    );
    let wtx = reopened
        .begin_write()
        .expect("inspect postcommit resource state");
    let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
    let current = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
        .unwrap()
        .expect("postcommit host survives");
    assert_eq!(
        current.held_cpu_weight,
        host().held_cpu_weight + request.requirement.cpu_weight
    );
    drop(hosts);
    wtx.commit().expect("commit postcommit inspection");
    #[cfg(feature = "security")]
    {
        let audit = verify_audit(&reopened, "graph-a").expect("verify resource audit");
        assert!(audit.ok);
        assert!(
            audit.entries > 0,
            "accepted resource mutation must be audited"
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn disk_hysteresis_uses_used_mib_boundaries() {
    assert!(!resource_disk_policy_blocked(
        false,
        799,
        Some(500),
        Some(800)
    ));
    assert!(resource_disk_policy_blocked(
        false,
        800,
        Some(500),
        Some(800)
    ));
    assert!(resource_disk_policy_blocked(
        true,
        501,
        Some(500),
        Some(800)
    ));
    assert!(!resource_disk_policy_blocked(
        true,
        500,
        Some(500),
        Some(800)
    ));
    // With equal watermarks, reopening at the shared boundary must not
    // immediately re-enter the open-state high check in the same evaluation.
    assert!(!resource_disk_policy_blocked(
        true,
        500,
        Some(500),
        Some(500)
    ));
}

#[test]
fn host_refresh_rejects_filesystem_shrink_under_existing_held_disk() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-host-refresh-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        let db = Database::create(&path).expect("create test database");
        let wtx = db.begin_write().expect("begin test write");
        initialize_canonical_tables(&wtx).expect("initialize tables");
        let mut nodes = wtx.open_table(NODES).unwrap();
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        let mut tenant_index = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).unwrap();
        let mut attempts = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).unwrap();
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let mut exclusivity = wtx.open_table(RESOURCE_EXCLUSIVITY).unwrap();
        let mut fairness = wtx.open_table(RESOURCE_FAIRNESS).unwrap();
        let mut concurrency = wtx.open_table(RESOURCE_CONCURRENCY).unwrap();
        let mut anti_affinity = wtx.open_table(RESOURCE_ANTI_AFFINITY).unwrap();
        let mut disk_policies = wtx.open_table(RESOURCE_DISK_POLICIES).unwrap();
        let crypto = DurableCrypto::none();
        let current = host();
        resource_put_host(&mut hosts, "graph-a", &current, crypto).unwrap();
        let refreshed = ResourceHostUpdateRequest {
            schema_version: crate::epistemic_operations::ResourceHostUpdateRequestSchemaVersion::V1,
            tenant_ref: "tenant-a".to_string(),
            host_ref: "host-1".to_string(),
            revision: current.revision + 1,
            capacity: current.capacity.clone(),
            observed: current.observed.clone(),
            heartbeat_at_ms: 1_000,
            heartbeat_ttl_ms: current.heartbeat_ttl_ms,
            now_ms: 2_000,
            draining: false,
            quarantined: false,
            labels: current.labels.clone(),
            target_kind: ResourceHostUpdateRequestTargetKind::Local,
            target_alias: None,
            disk_used_mib: 9_000,
            disk_capacity_mib: 9_100,
        };
        let result = apply_resource_reservation_rows(
            "graph-a",
            &Method::UpdateResourceHost { request: refreshed },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .unwrap()
        .expect("host update returns a typed refusal");
        assert!(matches!(result, crate::protocol::ResultPayload::Raw(_)));
        let after = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
            .unwrap()
            .expect("host remains present");
        assert_eq!(after.revision, current.revision);
        assert_eq!(after.disk_used_mib, current.disk_used_mib);
        assert_eq!(after.disk_capacity_mib, current.disk_capacity_mib);
        drop(nodes);
        drop(reservations);
        drop(tenant_index);
        drop(attempts);
        drop(hosts);
        drop(exclusivity);
        drop(fairness);
        drop(concurrency);
        drop(anti_affinity);
        drop(disk_policies);
        wtx.commit().expect("commit unchanged host state");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn host_disk_policy_projection_caps_at_schema_bound() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-policy-bound-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        let db = Database::create(&path).expect("create test database");
        let wtx = db.begin_write().expect("begin test write");
        initialize_canonical_tables(&wtx).expect("initialize tables");
        let mut nodes = wtx.open_table(NODES).unwrap();
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        let mut tenant_index = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).unwrap();
        let mut attempts = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).unwrap();
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let mut exclusivity = wtx.open_table(RESOURCE_EXCLUSIVITY).unwrap();
        let mut fairness = wtx.open_table(RESOURCE_FAIRNESS).unwrap();
        let mut concurrency = wtx.open_table(RESOURCE_CONCURRENCY).unwrap();
        let mut anti_affinity = wtx.open_table(RESOURCE_ANTI_AFFINITY).unwrap();
        let mut disk_policies = wtx.open_table(RESOURCE_DISK_POLICIES).unwrap();
        let crypto = DurableCrypto::none();
        let current = host();
        resource_put_host(&mut hosts, "graph-a", &current, crypto).unwrap();
        let policy = DurableResourceDiskPolicy {
            blocked: false,
            low_watermark_mib: Some(500),
            high_watermark_mib: Some(800),
            revision: 1,
        };
        for index in 0..128 {
            let key = format!("host-1\0policy-{index:03}");
            let bytes = resource_encode(&policy, crypto).unwrap();
            disk_policies
                .insert(("graph-a", key.as_str()), bytes.as_slice())
                .unwrap();
        }
        let update = |revision| ResourceHostUpdateRequest {
            schema_version: crate::epistemic_operations::ResourceHostUpdateRequestSchemaVersion::V1,
            tenant_ref: "tenant-a".to_string(),
            host_ref: "host-1".to_string(),
            revision,
            capacity: current.capacity.clone(),
            observed: current.observed.clone(),
            heartbeat_at_ms: 1_000,
            heartbeat_ttl_ms: current.heartbeat_ttl_ms,
            now_ms: 2_000,
            draining: false,
            quarantined: false,
            labels: current.labels.clone(),
            target_kind: ResourceHostUpdateRequestTargetKind::Local,
            target_alias: None,
            disk_used_mib: current.disk_used_mib,
            disk_capacity_mib: current.disk_capacity_mib,
        };
        let mut invalid_ttl = update(8);
        invalid_ttl.heartbeat_ttl_ms = 999;
        let error = apply_resource_reservation_rows(
            "graph-a",
            &Method::UpdateResourceHost {
                request: invalid_ttl,
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .expect_err("heartbeat TTL below the schema minimum must fail closed");
        assert!(error.contains("telemetry bounds"));
        apply_resource_reservation_rows(
            "graph-a",
            &Method::UpdateResourceHost { request: update(8) },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .expect("128 policy rows remain representable")
        .expect("host update result");
        assert_eq!(
            resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .unwrap()
                .revision,
            8
        );

        let key = "host-1\0policy-overflow";
        let bytes = resource_encode(&policy, crypto).unwrap();
        disk_policies
            .insert(("graph-a", key), bytes.as_slice())
            .unwrap();
        let error = apply_resource_reservation_rows(
            "graph-a",
            &Method::UpdateResourceHost { request: update(9) },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .expect_err("129 policy rows exceed the generated snapshot bound");
        assert!(error.contains("disk-policy scan exceeds native bound"));
        assert_eq!(
            resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .unwrap()
                .revision,
            8
        );
        drop(nodes);
        drop(reservations);
        drop(tenant_index);
        drop(attempts);
        drop(hosts);
        drop(exclusivity);
        drop(fairness);
        drop(concurrency);
        drop(anti_affinity);
        drop(disk_policies);
        wtx.commit()
            .expect("commit unchanged host after overflow refusal");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn orphan_attempt_index_fails_closed_without_recharging_host() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-orphan-attempt-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        let db = Database::create(&path).expect("create test database");
        let wtx = db.begin_write().expect("begin test write");
        initialize_canonical_tables(&wtx).expect("initialize tables");
        let mut nodes = wtx.open_table(NODES).unwrap();
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        let mut tenant_index = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).unwrap();
        let mut attempts = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).unwrap();
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let mut exclusivity = wtx.open_table(RESOURCE_EXCLUSIVITY).unwrap();
        let mut fairness = wtx.open_table(RESOURCE_FAIRNESS).unwrap();
        let mut concurrency = wtx.open_table(RESOURCE_CONCURRENCY).unwrap();
        let mut anti_affinity = wtx.open_table(RESOURCE_ANTI_AFFINITY).unwrap();
        let mut disk_policies = wtx.open_table(RESOURCE_DISK_POLICIES).unwrap();
        let crypto = DurableCrypto::none();
        let props = work_item_props();
        let props_bytes = rmp_serde::to_vec_named(&props).unwrap();
        nodes
            .insert(("graph-a", "work-1"), props_bytes.as_slice())
            .unwrap();
        let current_host = host();
        resource_put_host(&mut hosts, "graph-a", &current_host, crypto).unwrap();

        let mut reserve_request = request();
        reserve_request.reservation_id = "reservation-orphan".to_string();
        reserve_request.expected_lifecycle_revision = Some(0);
        reserve_request.input_fingerprint =
            resource_recomputed_fingerprint(&props, &reserve_request)
                .expect("test WorkItem has a complete resolved projection");
        attempts
            .insert(
                (
                    "graph-a",
                    reserve_request.work_item_id.as_str(),
                    reserve_request.attempt,
                ),
                reserve_request.reservation_id.as_str(),
            )
            .unwrap();

        let error = apply_resource_reservation_rows(
            "graph-a",
            &Method::ReserveWorkItemResources {
                request: reserve_request.clone(),
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .expect_err("an orphan attempt index is corruption, not an idempotent win");
        assert!(error.contains("attempt index references missing reservation"));
        let after = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
            .unwrap()
            .expect("host remains present");
        assert_eq!(after.held_cpu_weight, current_host.held_cpu_weight);
        assert_eq!(after.held_memory_mib, current_host.held_memory_mib);
        assert_eq!(after.held_disk_mib, current_host.held_disk_mib);
        assert_eq!(after.held_process_slots, current_host.held_process_slots);
        assert!(reservations
            .get(("graph-a", reserve_request.reservation_id.as_str()))
            .unwrap()
            .is_none());

        // Repair the deliberately injected orphan in this isolated database,
        // then exercise the real WTX reserve/release path.  The first reserve
        // must be the only operation that charges host capacity and counters.
        attempts
            .remove((
                "graph-a",
                reserve_request.work_item_id.as_str(),
                reserve_request.attempt,
            ))
            .unwrap();
        let accepted = apply_resource_reservation_rows(
            "graph-a",
            &Method::ReserveWorkItemResources {
                request: reserve_request.clone(),
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .unwrap()
        .expect("reserve result");
        let accepted = resource_decode_result_payload(accepted).unwrap();
        assert_eq!(
            accepted.decision,
            ResourceReservationResultDecision::Accepted
        );
        assert_eq!(
            accepted.held_cpu_weight,
            reserve_request.requirement.cpu_weight
        );
        let reserved_host = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
            .unwrap()
            .expect("reserved host");
        assert_eq!(
            reserved_host.held_cpu_weight,
            current_host.held_cpu_weight + 2
        );

        for expected in [Some(0), Some(2)] {
            let mut stale_release = reserve_request.clone();
            stale_release.expected_lifecycle_revision = expected;
            stale_release.now_ms = 2_000;
            let stale = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReleaseWorkItemResources {
                    request: stale_release,
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("stale lifecycle refusal result");
            assert_eq!(
                resource_decode_result_payload(stale).unwrap().decision,
                ResourceReservationResultDecision::Stale
            );
        }

        let mut release_request = reserve_request.clone();
        release_request.now_ms = 2_000;
        release_request.expected_lifecycle_revision = Some(1);
        let released = apply_resource_reservation_rows(
            "graph-a",
            &Method::ReleaseWorkItemResources {
                request: release_request.clone(),
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .unwrap()
        .expect("release result");
        let released = resource_decode_result_payload(released).unwrap();
        assert_eq!(
            released.decision,
            ResourceReservationResultDecision::Accepted
        );
        assert_eq!(released.held_cpu_weight, 0);
        assert_eq!(released.state, ResourceReservationResultState::Released);
        let released_host = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
            .unwrap()
            .expect("released host");
        assert_eq!(released_host.held_cpu_weight, current_host.held_cpu_weight);

        // The retained tombstone makes the exact release replay idempotent,
        // while a new reservation identity for the same WorkItem attempt is a
        // conflict with the durable attempt winner.
        let replay = apply_resource_reservation_rows(
            "graph-a",
            &Method::ReleaseWorkItemResources {
                request: release_request,
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .unwrap()
        .expect("release replay result");
        assert_eq!(
            resource_decode_result_payload(replay).unwrap().decision,
            ResourceReservationResultDecision::Idempotent
        );
        let mut changed_precondition = reserve_request.clone();
        changed_precondition.now_ms = 3_000;
        changed_precondition.expected_lifecycle_revision = Some(2);
        let changed_precondition_result = apply_resource_reservation_rows(
            "graph-a",
            &Method::ReleaseWorkItemResources {
                request: changed_precondition,
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .unwrap()
        .expect("changed lifecycle refusal result");
        assert_eq!(
            resource_decode_result_payload(changed_precondition_result)
                .unwrap()
                .decision,
            ResourceReservationResultDecision::InputConflict
        );
        let mut changed_id = reserve_request.clone();
        changed_id.reservation_id = "reservation-changed".to_string();
        changed_id.idempotency_key = "reserve-invocation-changed".to_string();
        changed_id.input_fingerprint =
            resource_recomputed_fingerprint(&props, &changed_id).unwrap();
        let conflict = apply_resource_reservation_rows(
            "graph-a",
            &Method::ReserveWorkItemResources {
                request: changed_id,
            },
            &mut nodes,
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .unwrap()
        .expect("changed-id refusal result");
        assert_eq!(
            resource_decode_result_payload(conflict).unwrap().decision,
            ResourceReservationResultDecision::Conflict
        );
        let after_conflict = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
            .unwrap()
            .expect("host remains after changed-id refusal");
        assert_eq!(after_conflict.held_cpu_weight, current_host.held_cpu_weight);
        drop(nodes);
        drop(reservations);
        drop(tenant_index);
        drop(attempts);
        drop(hosts);
        drop(exclusivity);
        drop(fairness);
        drop(concurrency);
        drop(anti_affinity);
        drop(disk_policies);
        wtx.commit()
            .expect("commit reserve/release transaction after orphan refusal");
    }
    // Reopen the durable store and rebuild the scheduler projection solely
    // from the native tombstone/status readers.  Held totals remain zero after
    // release, while exact lifecycle correlation still returns the record.
    {
        let db = Database::open(&path).expect("reopen resource database");
        let stored_request = request();
        let query = ResourceReservationStatusRequest {
            schema_version:
                crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: stored_request.tenant_ref.clone(),
            work_item_id: Some(stored_request.work_item_id.clone()),
            reservation_id: Some("reservation-orphan".to_string()),
            host_ref: Some(stored_request.host_ref.clone()),
            owner_id: Some(stored_request.owner_id.clone()),
            fence: Some(stored_request.fence.clone()),
            attempt: Some(stored_request.attempt),
            lease_epoch: Some(stored_request.lease_epoch),
            fencing_token: Some(stored_request.fencing_token),
            input_fingerprint: None,
            fairness_group: Some(stored_request.fairness_group.clone()),
            limit: 10,
            cursor: None,
            now_ms: 3_000,
        };
        let exact = read_resource_reservation(&db, "graph-a", &query, DurableCrypto::none())
            .expect("exact tombstone query");
        assert_eq!(
            exact.decision,
            ResourceReservationResultDecision::Idempotent
        );
        assert_eq!(exact.state, ResourceReservationResultState::Released);
        assert_eq!(exact.held_cpu_weight, 0);
        let mut wrong_host_query = query.clone();
        wrong_host_query.host_ref = Some("host-2".to_string());
        assert_eq!(
            read_resource_reservation(&db, "graph-a", &wrong_host_query, DurableCrypto::none())
                .expect_err("wrong host correlation must fail closed"),
            "resource reservation correlation does not match"
        );
        let status =
            read_resource_reservation_status(&db, "graph-a", &query, DurableCrypto::none())
                .expect("status projection after restart");
        assert!(status.complete);
        assert_eq!(status.reservations.len(), 1);
        assert_eq!(status.reservations[0].held_cpu_weight, 0);
        assert_eq!(
            status.reservations[0].state,
            ResourceReservationSummaryState::Released
        );
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn exact_request_record_match_binds_fence_and_all_immutable_policy() {
    let request = request();
    let record = resource_build_record(&request, &host(), 7, 1).unwrap();
    assert!(resource_request_matches_record(&request, &record));
    let mut changed_fence = request.clone();
    changed_fence.fence = "2".to_string();
    assert!(!resource_request_matches_record(&changed_fence, &record));
    let mut changed_input = request.clone();
    changed_input.input_fingerprint = "v1:changed".to_string();
    assert!(!resource_request_matches_record(&changed_input, &record));
    let mut changed_reserved_at = request.clone();
    changed_reserved_at.reserved_at_ms += 1;
    changed_reserved_at.expires_at_ms += 1;
    assert!(!resource_request_matches_record(
        &changed_reserved_at,
        &record
    ));
    let mut changed_host_precondition = request.clone();
    changed_host_precondition.expected_host_revision = Some(8);
    assert!(!resource_request_matches_record(
        &changed_host_precondition,
        &record
    ));
    let mut changed_lifecycle_precondition = request.clone();
    changed_lifecycle_precondition.expected_lifecycle_revision = Some(1);
    // Lifecycle CAS is checked by the release/reclaim transaction against the
    // current row, not treated as immutable reserve identity.
    assert!(resource_request_matches_record(
        &changed_lifecycle_precondition,
        &record
    ));
}

#[test]
fn work_item_validation_rejects_legacy_profile_and_future_fence_reclaim() {
    let request = request();
    let mut props = work_item_props();
    props["metadata"]["repository_work_item"]["resource_reservation"]
        .as_object_mut()
        .expect("resource extension")
        .remove("resolved_profile_authority");
    assert!(matches!(
        resource_validate_work_item(&props, &request, false),
        Err(ResourceReservationResultDecision::Policy)
    ));

    props = work_item_props();
    props["metadata"]["repository_work_item"]["resource_reservation"]["profile_version"] =
        serde_json::Value::String(resource_b64_urlsafe("01"));
    assert!(matches!(
        resource_validate_work_item(&props, &request, false),
        Err(ResourceReservationResultDecision::Policy)
    ));

    props = work_item_props();
    let mut future_fence = request;
    future_fence.lease_epoch = 99;
    future_fence.fencing_token = 99;
    future_fence.fence = "99".to_string();
    assert!(matches!(
        resource_validate_work_item(&props, &future_fence, true),
        Err(ResourceReservationResultDecision::Stale)
    ));
}

#[test]
fn reclaim_proves_strictly_newer_attempt_and_current_query_requires_live_lease() {
    let request = request();
    let mut props = work_item_props();
    props["status"] = serde_json::json!("ready");
    props["attempt"] = serde_json::json!(2);
    props["lease_epoch"] = serde_json::json!(2);
    props["fencing_token"] = serde_json::json!(2);
    let fence = resource_validate_work_item(&props, &request, true)
        .expect("strictly newer attempt is a reclaim supersession proof");
    assert!(fence.superseded);

    let record = resource_build_record(&request, &host(), 7, 1).unwrap();
    props = work_item_props();
    props["status"] = serde_json::json!("succeeded");
    assert!(!resource_record_work_item_live(&props, &record, 1_000));
    props["status"] = serde_json::json!("running");
    assert!(resource_record_work_item_live(&props, &record, 1_000));
    props["lease_expires_at"] = serde_json::json!(1.0);
    assert!(!resource_record_work_item_live(&props, &record, 1_000));
}

#[test]
fn bounded_query_validation_rejects_untrusted_optional_fields() {
    let request = ResourceReservationStatusRequest {
        schema_version:
            crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
        tenant_ref: "tenant-a".to_string(),
        work_item_id: Some("work-1".to_string()),
        reservation_id: None,
        host_ref: None,
        owner_id: None,
        fence: None,
        attempt: None,
        lease_epoch: None,
        fencing_token: None,
        input_fingerprint: Some("not-a-fingerprint".to_string()),
        fairness_group: None,
        limit: 1,
        cursor: None,
        now_ms: 1,
    };
    assert!(resource_validate_query_request(&request, true).is_err());
    let mut oversized = request;
    oversized.input_fingerprint = None;
    oversized.work_item_id = Some("x".repeat(MAX_RESOURCE_TEXT + 1));
    assert!(resource_validate_query_request(&oversized, true).is_err());
}

#[test]
fn exact_query_missing_reservation_returns_typed_not_found() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-query-missing-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        let db = Database::create(&path).expect("create query database");
        let wtx = db.begin_write().expect("begin query write");
        initialize_canonical_tables(&wtx).expect("initialize tables");
        wtx.commit().expect("commit empty query database");

        let expected = request();
        let query = ResourceReservationStatusRequest {
            schema_version:
                crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: expected.tenant_ref.clone(),
            work_item_id: Some(expected.work_item_id.clone()),
            reservation_id: Some("reservation-not-yet-created".to_string()),
            host_ref: Some(expected.host_ref.clone()),
            owner_id: Some(expected.owner_id.clone()),
            fence: Some(expected.fence.clone()),
            attempt: Some(expected.attempt),
            lease_epoch: Some(expected.lease_epoch),
            fencing_token: Some(expected.fencing_token),
            input_fingerprint: None,
            fairness_group: Some(expected.fairness_group.clone()),
            limit: 1,
            cursor: None,
            now_ms: expected.now_ms,
        };
        let result = read_resource_reservation(&db, "graph-a", &query, DurableCrypto::none())
            .expect("missing reservation is a typed result");
        assert_eq!(result.decision, ResourceReservationResultDecision::NotFound);
        assert_eq!(result.state, ResourceReservationResultState::Absent);
        assert!(result.record.is_none());
        assert_eq!(result.held_cpu_weight, 0);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn resource_scope_text_rejects_delimiter_and_control_collisions() {
    let mut embedded_delimiter = request();
    embedded_delimiter.repository_id = "repo\0branch".to_string();
    assert!(resource_validate_request(&embedded_delimiter).is_err());

    let mut embedded_newline = request();
    embedded_newline.fairness_group = "build\nteam".to_string();
    assert!(resource_validate_request(&embedded_newline).is_err());

    let mut query = ResourceReservationStatusRequest {
        schema_version:
            crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
        tenant_ref: "tenant\0a".to_string(),
        work_item_id: None,
        reservation_id: None,
        host_ref: None,
        owner_id: None,
        fence: None,
        attempt: None,
        lease_epoch: None,
        fencing_token: None,
        input_fingerprint: None,
        fairness_group: None,
        limit: 1,
        cursor: None,
        now_ms: 1,
    };
    assert!(resource_validate_query_request(&query, true).is_err());
    query.tenant_ref = "tenant-a".to_string();
    query.host_ref = Some("host\t1".to_string());
    assert!(resource_validate_query_request(&query, true).is_err());
}

#[test]
fn native_retry_comparison_normalizes_only_authoritative_time() {
    let first = Method::ReserveWorkItemResources { request: request() };
    let mut later_request = request();
    later_request.now_ms = 2_000;
    let later = Method::ReserveWorkItemResources {
        request: later_request,
    };
    assert_eq!(
        native_resource_retry_method_key(&first).unwrap(),
        native_resource_retry_method_key(&later).unwrap()
    );

    let mut changed_request = request();
    changed_request.fence = "2".to_string();
    let changed = Method::ReserveWorkItemResources {
        request: changed_request,
    };
    assert_ne!(
        native_resource_retry_method_key(&first).unwrap(),
        native_resource_retry_method_key(&changed).unwrap()
    );

    let operation = |method| MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Job,
        domain: MutationDomain::ControlPlane,
        method,
    };
    let stored = vec![operation(first)];
    let replayed = vec![operation(later)];
    assert!(mutation_operations_retry_match(&stored, &replayed).unwrap());
    let mut changed_host = request();
    changed_host.host_ref = "host-2".to_string();
    assert!(!mutation_operations_retry_match(
        &stored,
        &[operation(Method::ReserveWorkItemResources {
            request: changed_host,
        })]
    )
    .unwrap());

    let stored_batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: "batch-1".to_string(),
        context: MutationRequestContext {
            request_id: 1,
            principal: "principal:sha256:".to_string() + &"a".repeat(64),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: "tenant-a".to_string(),
        graph: "graph-a".to_string(),
        placement_epoch: 1,
        idempotency_key: "idem-1".to_string(),
        expected_graph_version: None,
        fencing_token: Some(1),
        authoritative_state: None,
        operations: stored,
        outbox: Vec::new(),
        created_at_ms: 1,
    };
    let mut failover_batch = stored_batch.clone();
    failover_batch.placement_epoch = 2;
    failover_batch.fencing_token = Some(1);
    assert!(native_resource_placement_replay_match(
        &stored_batch,
        &failover_batch,
        true
    ));
    let mut backwards = failover_batch.clone();
    backwards.placement_epoch = 0;
    assert!(!native_resource_placement_replay_match(
        &stored_batch,
        &backwards,
        true
    ));
    let mut fabricated_fence = failover_batch;
    fabricated_fence.fencing_token = Some(99);
    assert!(!native_resource_placement_replay_match(
        &stored_batch,
        &fabricated_fence,
        true
    ));
}

#[test]
fn native_retry_rebuilds_projection_outbox_after_authoritative_time_changes() {
    let operation = |method| MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Job,
        domain: MutationDomain::ControlPlane,
        method,
    };
    let stored_method = Method::ReserveWorkItemResources { request: request() };
    let mut later_request = request();
    later_request.now_ms = 2_000;
    let proposed_method = Method::ReserveWorkItemResources {
        request: later_request,
    };
    let stored_operations = vec![operation(stored_method)];
    let proposed_operations = vec![operation(proposed_method)];
    assert!(mutation_operations_retry_match(&stored_operations, &proposed_operations).unwrap());
    let stored_payload =
        crate::server::mutation_batch::projection_payload_for_operations(&stored_operations)
            .unwrap();
    let proposed_payload =
        crate::server::mutation_batch::projection_payload_for_operations(&proposed_operations)
            .unwrap();
    assert_ne!(
        stored_payload, proposed_payload,
        "the producer binds each outbox to its historical authority timestamp"
    );
    let metadata =
        std::collections::BTreeMap::from([("scope_sha256".to_string(), "digest".to_string())]);
    let stored_outbox = vec![MutationOutboxIntent {
        topic: "engine.projection.rebuild".to_string(),
        key: "batch-1".to_string(),
        payload: stored_payload,
        headers: metadata.clone(),
    }];
    let proposed_outbox = vec![MutationOutboxIntent {
        topic: "engine.projection.rebuild".to_string(),
        key: "batch-1".to_string(),
        payload: proposed_payload,
        headers: metadata,
    }];
    assert!(native_resource_retry_outbox_match(
        &stored_operations,
        &proposed_operations,
        &stored_outbox,
        &proposed_outbox,
        true,
    )
    .unwrap());
    let mut tampered = proposed_outbox;
    tampered[0].payload[0] ^= 1;
    assert!(!native_resource_retry_outbox_match(
        &stored_operations,
        &proposed_operations,
        &stored_outbox,
        &tampered,
        true,
    )
    .unwrap());
}

#[test]
fn target_policy_keeps_default_local_and_remote_preference_distinct() {
    let extension = serde_json::json!({
        "preferred_target": {
            "contract_version": "1",
            "kind": "local",
            "alias": null,
            "capability_labels": []
        }
    });
    let extension = extension.as_object().unwrap();
    assert!(resource_target_selection_matches(extension, &host()).unwrap());

    let mut remote = host();
    remote.target_kind = "inventory_alias".to_string();
    remote.target_alias = Some("remote-a".to_string());
    assert!(!resource_target_selection_matches(extension, &remote).unwrap());
}

#[test]
fn selected_host_identity_must_match_wire_target_pair() {
    let local_request = request();
    assert!(resource_selected_target_matches_request(
        &local_request,
        &host()
    ));

    let mut remote_host = host();
    remote_host.target_kind = "inventory_alias".to_string();
    remote_host.target_alias = Some("remote-a".to_string());
    let mut remote_request = local_request.clone();
    remote_request.target_kind = ResourceReservationRequestTargetKind::InventoryAlias;
    remote_request.target_alias = Some("remote-a".to_string());
    assert!(resource_selected_target_matches_request(
        &remote_request,
        &remote_host
    ));
    assert!(!resource_selected_target_matches_request(
        &local_request,
        &remote_host
    ));
    remote_request.target_alias = Some("remote-b".to_string());
    assert!(!resource_selected_target_matches_request(
        &remote_request,
        &remote_host
    ));
}

#[test]
fn work_item_target_projection_must_match_reservation_request() {
    let props = work_item_props();
    let repository = props
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .and_then(|metadata| metadata.get("repository_work_item"))
        .and_then(serde_json::Value::as_object)
        .expect("repository WorkItem metadata");
    let extension = repository
        .get("resource_reservation")
        .and_then(serde_json::Value::as_object)
        .expect("native resource extension");
    let request = request();
    assert!(resource_extension_matches(repository, extension, &request).unwrap());

    let mut remote_request = request.clone();
    remote_request.target_kind = ResourceReservationRequestTargetKind::InventoryAlias;
    remote_request.target_alias = Some("remote-a".to_string());
    // The original WorkItem declaration is local, but RM may select a remote
    // host when its preferred/required policy permits it; selection is bound
    // to the host row by `resource_selected_target_matches_request`.
    assert!(resource_extension_matches(repository, extension, &remote_request).unwrap());

    let mut remote_extension = extension.clone();
    remote_extension.insert(
        "target_kind".to_string(),
        serde_json::Value::String("inventory_alias".to_string()),
    );
    remote_extension.insert(
        "target_alias".to_string(),
        serde_json::Value::String(resource_b64_urlsafe("remote-a")),
    );
    assert!(!resource_extension_matches(repository, &remote_extension, &request).unwrap());

    let mut changed_digest = extension.clone();
    changed_digest.insert(
        "work_item_input_fingerprint".to_string(),
        serde_json::Value::String(format!("v1:{}", "2".repeat(64))),
    );
    assert!(!resource_extension_matches(repository, &changed_digest, &request).unwrap());
}

#[test]
fn canonical_opaque_decoder_rejects_noncanonical_chunks_and_accepts_unicode() {
    let encoded = resource_b64_urlsafe("é");
    assert_eq!(resource_b64_value(&encoded, "unicode").unwrap(), "é");
    assert!(resource_b64_value(&format!("{}.bad", encoded), "noncanonical").is_err());
}

#[test]
fn direct_request_validation_rejects_noncanonical_profile_version() {
    let mut request = request();
    request.profile_version = "01".to_string();
    let error = resource_validate_request(&request).expect_err("profile version must be canonical");
    assert!(error.contains("canonical integer"));
}

#[test]
fn terminal_result_replays_exact_record_without_held_capacity() {
    let request = request();
    let mut record = resource_build_record(&request, &host(), 7, 1).unwrap();
    record.state = ResourceReservationRecordState::Released;
    record.tombstone = true;
    let result = resource_decode_result_payload(resource_result_payload(
        ResourceReservationResultDecision::Idempotent,
        &request,
        Some(record.clone()),
        Some(&host()),
        9,
        Vec::new(),
    ))
    .unwrap();
    assert!(resource_request_matches_record(&request, &record));
    assert!(result.tombstone);
    assert_eq!(result.held_cpu_weight, 0);
    assert_eq!(result.held_memory_mib, 0);
    assert_eq!(result.held_disk_mib, 0);
    assert_eq!(result.held_process_slots, 0);
    assert_eq!(result.fairness_debt, 9);
}

#[test]
fn graph_clear_streams_terminal_history_past_bound_and_preserves_active_holds() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-clear-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        let db = Database::create(&path).expect("create test database");
        let wtx = db.begin_write().expect("begin test write");
        initialize_canonical_tables(&wtx).expect("initialize tables");
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        let mut tenant_index = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).unwrap();
        let mut attempts = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).unwrap();
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let mut exclusivity = wtx.open_table(RESOURCE_EXCLUSIVITY).unwrap();
        let mut fairness = wtx.open_table(RESOURCE_FAIRNESS).unwrap();
        let mut concurrency = wtx.open_table(RESOURCE_CONCURRENCY).unwrap();
        let mut anti_affinity = wtx.open_table(RESOURCE_ANTI_AFFINITY).unwrap();
        let mut disk_policies = wtx.open_table(RESOURCE_DISK_POLICIES).unwrap();
        let crypto = DurableCrypto::none();
        let base_request = request();
        let base_record = resource_build_record(&base_request, &host(), 7, 1).unwrap();

        for index in 0..=MAX_RESOURCE_CLEAR_SCAN {
            // A max-Unicode prefix followed by another byte sorts after the
            // old `..=\u{10ffff}` sentinel.  The production clear/status
            // ranges are open-ended and must still include this legal ID.
            let reservation_id = if index == MAX_RESOURCE_CLEAR_SCAN {
                "\u{10ffff}terminal-x".to_string()
            } else {
                format!("terminal-{index:06}")
            };
            let mut record = base_record.clone();
            record.reservation_id = reservation_id.clone();
            if index == 0 {
                record.tenant_ref = "\u{10ffff}tenant-x".to_string();
            }
            record.state = ResourceReservationRecordState::Released;
            record.tombstone = true;
            record.revision = index as u64 + 1;
            record.lifecycle_revision = index as u64 + 1;
            let tenant = record.tenant_ref.clone();
            let durable = DurableResourceReservation {
                record,
                held_cpu_weight: 0,
                held_memory_mib: 0,
                held_disk_mib: 0,
                held_process_slots: 0,
                fairness_debt: 1,
            };
            let bytes = resource_encode(&durable, crypto).unwrap();
            reservations
                .insert(("graph-a", reservation_id.as_str()), bytes.as_slice())
                .unwrap();
            tenant_index
                .insert(
                    ("graph-a", tenant.as_str(), reservation_id.as_str()),
                    reservation_id.as_str(),
                )
                .unwrap();
        }

        let mut max_host = host();
        max_host.host_ref = "\u{10ffff}host-x".to_string();
        hosts
            .insert(
                ("graph-a", max_host.host_ref.as_str()),
                resource_encode(&max_host, crypto).unwrap().as_slice(),
            )
            .unwrap();
        let max_policy_key = "\u{10ffff}policy-x";
        let max_policy = DurableResourceDiskPolicy {
            blocked: false,
            low_watermark_mib: Some(1),
            high_watermark_mib: Some(2),
            revision: 1,
        };
        let max_policy_bytes = resource_encode(&max_policy, crypto).unwrap();
        let max_policy_row = format!("host-a\0{max_policy_key}");
        disk_policies
            .insert(
                ("graph-a", max_policy_row.as_str()),
                max_policy_bytes.as_slice(),
            )
            .unwrap();

        let active_id = "active-hold";
        let mut active_request = request();
        active_request.reservation_id = active_id.to_string();
        let active_record = resource_build_record(&active_request, &host(), 7, 1).unwrap();
        let active = DurableResourceReservation {
            record: active_record,
            held_cpu_weight: active_request.requirement.cpu_weight,
            held_memory_mib: active_request.requirement.memory_mib,
            held_disk_mib: active_request.requirement.disk_mib,
            held_process_slots: active_request.requirement.process_slots,
            fairness_debt: active_request.fairness_cost,
        };
        let active_bytes = resource_encode(&active, crypto).unwrap();
        reservations
            .insert(("graph-a", active_id), active_bytes.as_slice())
            .unwrap();
        tenant_index
            .insert(("graph-a", "tenant-a", active_id), active_id)
            .unwrap();

        assert!(clear_resource_rows(
            "graph-a",
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .is_err());
        assert!(reservations.get(("graph-a", active_id)).unwrap().is_some());
        assert!(tenant_index
            .get(("graph-a", "tenant-a", active_id))
            .unwrap()
            .is_some());

        reservations.remove(("graph-a", active_id)).unwrap();
        tenant_index
            .remove(("graph-a", "tenant-a", active_id))
            .unwrap();
        clear_resource_rows(
            "graph-a",
            &mut reservations,
            &mut tenant_index,
            &mut attempts,
            &mut hosts,
            &mut exclusivity,
            &mut fairness,
            &mut concurrency,
            &mut anti_affinity,
            &mut disk_policies,
            crypto,
        )
        .expect("terminal history is cleared in bounded chunks");
        assert!(reservations
            .range(("graph-a", "")..)
            .unwrap()
            .next()
            .is_none());
        assert!(tenant_index
            .range(("graph-a", "", "")..)
            .unwrap()
            .next()
            .is_none());
        drop(reservations);
        drop(tenant_index);
        drop(attempts);
        drop(hosts);
        drop(exclusivity);
        drop(fairness);
        drop(concurrency);
        drop(anti_affinity);
        drop(disk_policies);
        wtx.commit().expect("commit clear");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn checkpoint_replaces_graph_rows_but_preserves_native_resource_domain() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-checkpoint-domain-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        let db = Database::create(&path).expect("create checkpoint database");
        let wtx = db.begin_write().expect("begin checkpoint seed");
        initialize_canonical_tables(&wtx).expect("initialize tables");
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let current_host = host();
        resource_put_host(&mut hosts, "graph-a", &current_host, DurableCrypto::none()).unwrap();
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        let mut request = request();
        request.input_fingerprint = format!("v1:{}", "0".repeat(64));
        let record = resource_build_record(&request, &current_host, current_host.revision, 1)
            .expect("build active checkpoint record");
        let durable = DurableResourceReservation {
            record,
            held_cpu_weight: request.requirement.cpu_weight,
            held_memory_mib: request.requirement.memory_mib,
            held_disk_mib: request.requirement.disk_mib,
            held_process_slots: request.requirement.process_slots,
            fairness_debt: request.fairness_cost,
        };
        resource_put_reservation(
            &mut reservations,
            "graph-a",
            &durable,
            DurableCrypto::none(),
        )
        .unwrap();
        let mut tenant_index = wtx.open_table(RESOURCE_RESERVATION_TENANT_INDEX).unwrap();
        tenant_index
            .insert(
                (
                    "graph-a",
                    request.tenant_ref.as_str(),
                    request.reservation_id.as_str(),
                ),
                request.reservation_id.as_str(),
            )
            .unwrap();
        let mut attempts = wtx.open_table(RESOURCE_RESERVATION_ATTEMPTS).unwrap();
        attempts
            .insert(
                ("graph-a", request.work_item_id.as_str(), request.attempt),
                request.reservation_id.as_str(),
            )
            .unwrap();
        drop(hosts);
        drop(reservations);
        drop(tenant_index);
        drop(attempts);
        wtx.commit().expect("commit checkpoint seed");

        let work_item_bytes = rmp_serde::to_vec_named(&work_item_props_for_request(&request))
            .expect("encode linked WorkItem");
        let make_dump = |incarnation_id: &str,
                         source_snapshot_version: u64,
                         nodes: Vec<(String, Vec<u8>)>| GraphDump {
            graph: "graph-a".to_string(),
            name: "graph-a".to_string(),
            graph_type: GraphType::Global,
            incarnation_id: incarnation_id.to_string(),
            source_snapshot_version,
            integrity_policy: None,
            nodes,
            edges: Vec::new(),
            ledger: Vec::new(),
            semantic: Vec::new(),
        };

        // Establish a valid image first.  The incoming snapshot contains the
        // exact live WorkItem linked by the native hold.
        assert_eq!(
            apply_checkpoint(
                &db,
                &mut Vec::new(),
                vec![make_dump(
                    "incarnation:checkpoint-domain-initial",
                    10,
                    vec![
                        (
                            "old-node".to_string(),
                            rmp_serde::to_vec_named(&serde_json::json!({"old": true})).unwrap(),
                        ),
                        (request.work_item_id.clone(), work_item_bytes.clone()),
                    ],
                )],
                DurableCrypto::none(),
            )
            .unwrap(),
            1
        );
        let initial = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("checkpoint graph identity");
        assert_eq!(
            initial
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["old-node", "work-1"]
        );

        // The incoming replacement omits the linked WorkItem.  It must refuse
        // before clear_graph_rows, leaving both the old graph image and native
        // held authority untouched.
        let error = apply_checkpoint(
            &db,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-invalid",
                11,
                vec![(
                    "new-node".to_string(),
                    rmp_serde::to_vec_named(&serde_json::json!({"new": true})).unwrap(),
                )],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot orphan an active native hold");
        assert_eq!(error, "checkpoint resource domain validation failed");
        assert!(!error.contains("reservation-1"));
        assert!(!error.contains("work-1"));
        let refused = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("graph remains after refused checkpoint");
        assert_eq!(
            refused
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["old-node", "work-1"]
        );

        // A replacement containing the exact linked WorkItem is valid and can
        // replace the ordinary graph rows while preserving the native domain.
        assert_eq!(
            apply_checkpoint(
                &db,
                &mut Vec::new(),
                vec![make_dump(
                    "incarnation:checkpoint-domain-valid",
                    12,
                    vec![
                        (
                            "new-node".to_string(),
                            rmp_serde::to_vec_named(&serde_json::json!({"new": true})).unwrap(),
                        ),
                        (request.work_item_id.clone(), work_item_bytes.clone()),
                    ],
                )],
                DurableCrypto::none(),
            )
            .unwrap(),
            1
        );
        let restored = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("valid replacement graph identity");
        assert_eq!(
            restored
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["new-node", "work-1"]
        );

        // A replacement image may not move the graph authority backwards,
        // even when it contains an otherwise valid live WorkItem.
        let stale_version = apply_checkpoint(
            &db,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-stale-version",
                11,
                vec![(request.work_item_id.clone(), work_item_bytes.clone())],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot lower the graph snapshot version");
        assert_eq!(stale_version, "checkpoint graph image is stale");

        // A historically valid WorkItem image with an old fence cannot be
        // restored beside the still-held reservation.
        let mut old_fence_props = work_item_props_for_request(&request);
        old_fence_props.insert("fencing_token".to_string(), serde_json::json!(0));
        old_fence_props.insert("lease_epoch".to_string(), serde_json::json!(0));
        let old_fence = apply_checkpoint(
            &db,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-old-fence",
                13,
                vec![(
                    request.work_item_id.clone(),
                    rmp_serde::to_vec_named(&old_fence_props).unwrap(),
                )],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot restore an old WorkItem fence");
        assert_eq!(old_fence, "checkpoint resource domain validation failed");

        // A lease expiring at or before reservation time cannot keep a held
        // reservation alive.
        let mut expired_props = work_item_props_for_request(&request);
        expired_props.insert("lease_expires_at".to_string(), serde_json::json!(1.0));
        let expired = apply_checkpoint(
            &db,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-expired-at-reservation",
                14,
                vec![(
                    request.work_item_id.clone(),
                    rmp_serde::to_vec_named(&expired_props).unwrap(),
                )],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint rejects a WorkItem lease expired at/before reservation time");
        assert_eq!(expired, "checkpoint resource domain validation failed");
        let unchanged = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("graph remains after stale resource checkpoint images");
        assert_eq!(unchanged.source_snapshot_version, 12);
        assert_eq!(
            unchanged
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["new-node", "work-1"]
        );

        // Checkpoint validity is historical: 2.0s is later than the
        // reservation at 1.0s, even though it is far in the past relative to
        // this test's real wall clock. Later expiry/reclaim is handled only by
        // an explicit authoritative transaction carrying `now_ms`.
        let mut historically_valid_props = work_item_props_for_request(&request);
        historically_valid_props.insert("lease_expires_at".to_string(), serde_json::json!(2.0));
        assert_eq!(
            apply_checkpoint(
                &db,
                &mut Vec::new(),
                vec![make_dump(
                    "incarnation:checkpoint-domain-historical-expiry",
                    15,
                    vec![(
                        request.work_item_id.clone(),
                        rmp_serde::to_vec_named(&historically_valid_props).unwrap(),
                    ),],
                )],
                DurableCrypto::none(),
            )
            .expect("checkpoint accepts a lease valid after reservation time"),
            1
        );
        let historically_installed = read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("historically valid checkpoint remains installed");
        assert_eq!(historically_installed.source_snapshot_version, 15);

        let wtx = db.begin_write().expect("inspect preserved resource domain");
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        assert!(
            resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
                .unwrap()
                .is_some()
        );
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        assert!(resource_load_reservation(
            &mut reservations,
            "graph-a",
            "reservation-1",
            DurableCrypto::none()
        )
        .unwrap()
        .is_some());
        drop(hosts);
        drop(reservations);
        wtx.commit().expect("commit resource inspection");

        // A graph lifecycle clear cannot silently strand this held domain. The
        // pending clear is rejected atomically and leaves both graph/resource
        // rows untouched until release/reclaim drains the hold.
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];
        let error = apply_checkpoint(&db, &mut pending, Vec::new(), DurableCrypto::none())
            .expect_err("active resource hold blocks checkpoint clear");
        assert!(error.contains("native reservation rows to be drained"));
        assert_eq!(
            pending.len(),
            1,
            "failed checkpoint preserves caller pending work"
        );
        assert!(matches!(pending[0].1, Method::ClearGraph));
        let wtx = db.begin_write().expect("inspect failed clear");
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        assert!(resource_load_reservation(
            &mut reservations,
            "graph-a",
            "reservation-1",
            DurableCrypto::none()
        )
        .unwrap()
        .is_some());
        drop(reservations);
        wtx.commit().expect("commit failed-clear inspection");
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn delete_graph_with_active_native_hold_is_atomic_and_recreate_is_clean() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-delete-active-hold-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let db = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );

    let reserve = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-delete-active-reserve",
        "delete-active-reserve",
    );
    let reserved = commit_resource_batch(&db, &reserve).expect("reserve active hold");
    let reserved_result = batch_resource_result(&reserved);
    assert_eq!(
        reserved_result.decision,
        ResourceReservationResultDecision::Accepted
    );
    assert!(reserved_result.held_cpu_weight > 0);

    let lifecycle_batch = |method: Method, batch_id: &str, idempotency_key: &str| {
        let mut batch = resource_batch(&request.tenant_ref, method, batch_id, idempotency_key);
        batch.operations[0].surface = MutationSurface::Lifecycle;
        batch.operations[0].domain = MutationDomain::Lifecycle;
        batch
    };

    // DeleteGraph must fail before its graph/resource row changes become
    // durable while a native hold is still active.  No lifecycle status or
    // projection outbox row may survive the failed transaction either.
    let delete_while_held = lifecycle_batch(
        Method::DeleteGraph {
            graph_name: "graph-a".to_string(),
        },
        "batch-delete-active-held",
        "delete-active-held",
    );
    let error = commit_resource_batch(&db, &delete_while_held)
        .expect_err("active native hold blocks DeleteGraph atomically");
    assert_eq!(
        error,
        "resource graph clear requires native reservation rows to be drained"
    );
    assert!(
        read_mutation_batch(&db, &delete_while_held.batch_id, DurableCrypto::none())
            .unwrap()
            .is_none()
    );
    assert!(
        read_mutation_outbox(&db, &delete_while_held.batch_id, DurableCrypto::none())
            .unwrap()
            .is_empty()
    );
    assert!(
        read_one_node(&db, "graph-a", &request.work_item_id, DurableCrypto::none())
            .unwrap()
            .is_some()
    );
    {
        let wtx = db
            .begin_write()
            .expect("inspect held rows after failed delete");
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        let held = resource_load_reservation(
            &mut reservations,
            "graph-a",
            &request.reservation_id,
            DurableCrypto::none(),
        )
        .unwrap()
        .expect("active reservation survives failed DeleteGraph");
        assert_eq!(held.record.state, ResourceReservationRecordState::Reserved);
        assert!(held.held_cpu_weight > 0);
        drop(reservations);
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        let held_host = resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
            .unwrap()
            .expect("host accounting survives failed DeleteGraph");
        assert_eq!(
            held_host.held_cpu_weight,
            host().held_cpu_weight + request.requirement.cpu_weight
        );
        drop(hosts);
        wtx.commit().expect("commit held-row inspection");
    }

    // Drain the hold through its explicit lifecycle operation, then the same
    // DeleteGraph path may remove the graph and all terminal resource history.
    let mut release_request = request.clone();
    release_request.now_ms = 2_000;
    release_request.expected_lifecycle_revision = Some(1);
    let release_tenant_ref = release_request.tenant_ref.clone();
    let release = resource_batch(
        &release_tenant_ref,
        Method::ReleaseWorkItemResources {
            request: release_request,
        },
        "batch-delete-active-release",
        "delete-active-release",
    );
    let released = commit_resource_batch(&db, &release).expect("release active hold");
    assert_eq!(
        batch_resource_result(&released).decision,
        ResourceReservationResultDecision::Accepted
    );

    let delete_after_release = lifecycle_batch(
        Method::DeleteGraph {
            graph_name: "graph-a".to_string(),
        },
        "batch-delete-after-release",
        "delete-after-release",
    );
    commit_resource_batch(&db, &delete_after_release)
        .expect("DeleteGraph succeeds after explicit hold drain");
    assert!(
        read_one_node(&db, "graph-a", &request.work_item_id, DurableCrypto::none())
            .unwrap()
            .is_none()
    );
    {
        let wtx = db
            .begin_write()
            .expect("inspect rows after successful delete");
        let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
        assert!(resource_load_reservation(
            &mut reservations,
            "graph-a",
            &request.reservation_id,
            DurableCrypto::none(),
        )
        .unwrap()
        .is_none());
        drop(reservations);
        let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
        assert!(
            resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
                .unwrap()
                .is_none()
        );
        drop(hosts);
        wtx.commit().expect("commit deleted-row inspection");
    }

    // Recreate the same graph name.  The fresh lifecycle must not recover the
    // old WorkItem, reservation, or terminal tombstone from the deleted image.
    let recreate = lifecycle_batch(
        Method::CreateGraph {
            graph_name: "graph-a".to_string(),
            graph_type: GraphType::Global,
        },
        "batch-recreate-after-delete",
        "recreate-after-delete",
    );
    commit_resource_batch(&db, &recreate).expect("recreate graph after drained delete");
    assert!(
        read_one_node(&db, "graph-a", &request.work_item_id, DurableCrypto::none())
            .unwrap()
            .is_none()
    );
    let meta = read_all_graph_meta(&db).unwrap();
    assert!(meta.iter().any(|(graph, _, _, incarnation)| {
        graph == "graph-a" && incarnation == &recreate.batch_id
    }));
    let wtx = db.begin_write().expect("inspect recreated resource domain");
    let mut reservations = wtx.open_table(RESOURCE_RESERVATIONS).unwrap();
    assert!(resource_load_reservation(
        &mut reservations,
        "graph-a",
        &request.reservation_id,
        DurableCrypto::none(),
    )
    .unwrap()
    .is_none());
    let mut hosts = wtx.open_table(RESOURCE_HOSTS).unwrap();
    assert!(
        resource_load_host(&mut hosts, "graph-a", "host-1", DurableCrypto::none())
            .unwrap()
            .is_none()
    );
    drop(hosts);
    drop(reservations);
    wtx.commit().expect("commit recreated-row inspection");

    drop(db);
    let _ = std::fs::remove_file(path);
}

#[test]
fn rmdd08_reservation_fingerprint_matches_cross_language_golden_vector() {
    // Generated from Repository Manager's frozen
    // _reservation_input_fingerprint using a ResourceProfile version 3 and
    // ResourceRequest.model_dump(mode="json").  Keep this fixture beside the
    // Rust recomputation so integer profile versions, contract markers, target
    // policy markers, and UTC Z deadline spelling cannot drift independently.
    let mut request = request();
    request.work_item_id =
        "workitem:repository_manager:11111111-1111-1111-1111-111111111111".to_string();
    request.attempt = 2;
    request.fence = "7".to_string();
    request.lease_epoch = 7;
    request.fencing_token = 7;
    request.reservation_id = "reservation:golden".to_string();
    request.owner_id = "owner-a".to_string();
    request.tenant_ref = "tenant-a".to_string();
    request.profile_version = "3".to_string();
    request.requirement = ResourceRequirement {
        cpu_weight: 5,
        memory_mib: 3_072,
        disk_mib: 5_000,
        process_slots: 3,
    };
    request.repository_id = "repo-opaque".to_string();
    request.branch = "release/main".to_string();
    request.concurrency_key = "rust-build".to_string();
    request.concurrency_limit = Some(2);
    request.required_labels = vec!["linux".to_string(), "rust".to_string()];
    request.anti_affinity = vec!["compiler".to_string(), "gpu".to_string()];
    request.fairness_group = "build".to_string();
    request.disk_low_watermark_mib = Some(400);
    request.disk_high_watermark_mib = Some(700);
    request.disk_policy_key = "rust-v3".to_string();
    let mut props = work_item_props();
    let repository = props["metadata"]["repository_work_item"]
        .as_object_mut()
        .expect("repository metadata");
    repository["tenant_id"] = serde_json::Value::String(resource_b64_urlsafe("tenant-a"));
    repository["repository_id"] = serde_json::Value::String(resource_b64_urlsafe("repo-opaque"));
    repository["owner_id"] = serde_json::Value::String(resource_b64_urlsafe("owner-a"));
    repository["branch"] = serde_json::Value::String(resource_b64_urlsafe("release/main"));
    repository["job_id"] = serde_json::Value::String(resource_b64_urlsafe(
        "rmjob:22222222-2222-2222-2222-222222222222",
    ));
    repository["priority"] = serde_json::json!(17);
    repository["queue_deadline"] = serde_json::json!("2026-08-09T12:00:00Z");
    let extension = repository["resource_reservation"]
        .as_object_mut()
        .expect("resource extension");
    extension["profile_name"] = serde_json::Value::String(resource_b64_urlsafe("rust-build"));
    extension["profile_version"] = serde_json::Value::String(resource_b64_urlsafe("3"));
    extension["cpu_weight"] = serde_json::json!(5);
    extension["memory_mib"] = serde_json::json!(3_072);
    extension["disk_mib"] = serde_json::json!(5_000);
    extension["process_slots"] = serde_json::json!(3);
    extension["host_labels"] =
        serde_json::json!([resource_b64_urlsafe("linux"), resource_b64_urlsafe("rust")]);
    extension["anti_affinity"] = serde_json::json!([
        resource_b64_urlsafe("compiler"),
        resource_b64_urlsafe("gpu")
    ]);
    extension["repository_id"] = serde_json::Value::String(resource_b64_urlsafe("repo-opaque"));
    extension["concurrency_key"] = serde_json::Value::String(resource_b64_urlsafe("rust-build"));
    extension["concurrency_limit"] = serde_json::json!(2);
    extension["repository_exclusive"] = serde_json::json!(true);
    extension["branch_exclusive"] = serde_json::json!(true);
    extension["fairness_group"] = serde_json::Value::String(resource_b64_urlsafe("build"));
    extension["disk_policy_key"] = serde_json::Value::String(resource_b64_urlsafe("rust-v3"));
    extension["disk_low_watermark_mib"] = serde_json::json!(400);
    extension["disk_high_watermark_mib"] = serde_json::json!(700);
    extension["branch"] = serde_json::Value::String(resource_b64_urlsafe("release/main"));
    extension["base_ref"] = serde_json::Value::String(resource_b64_urlsafe("release/main"));
    let actual = resource_recomputed_fingerprint(&props, &request).unwrap();
    assert_eq!(
        actual,
        "v1:13553590ebbcc2eca94df4968dcc1df773b847ed23cc833ec91350406b4067bb"
    );
}
