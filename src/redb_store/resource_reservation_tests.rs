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

#[test]
fn capacity_accounts_for_live_observed_usage_and_checked_last_slot() {
    let host = host();
    let requirement = ResourceRequirement {
        cpu_weight: 3,
        memory_mib: 5_120,
        disk_mib: 9_700,
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
    let mut changed_reserved_at = request;
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
    let mut changed_lifecycle_precondition = request;
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
    assert_eq!(
        resource_validate_work_item(&props, &request, false),
        Err(ResourceReservationResultDecision::Policy)
    );

    props = work_item_props();
    props["metadata"]["repository_work_item"]["resource_reservation"]["profile_version"] =
        serde_json::Value::String(resource_b64_urlsafe("01"));
    assert_eq!(
        resource_validate_work_item(&props, &request, false),
        Err(ResourceReservationResultDecision::Policy)
    );

    props = work_item_props();
    let mut future_fence = request;
    future_fence.lease_epoch = 99;
    future_fence.fencing_token = 99;
    future_fence.fence = "99".to_string();
    assert_eq!(
        resource_validate_work_item(&props, &future_fence, true),
        Err(ResourceReservationResultDecision::Stale)
    );
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
    let normalized = native_resource_retry_operations(&stored_operations).unwrap();
    let payload =
        crate::server::mutation_batch::projection_payload_for_operations(&normalized).unwrap();
    let metadata =
        std::collections::BTreeMap::from([("scope_sha256".to_string(), "digest".to_string())]);
    let stored_outbox = vec![MutationOutboxIntent {
        topic: "engine.projection.rebuild".to_string(),
        key: "batch-1".to_string(),
        payload: payload.clone(),
        headers: metadata.clone(),
    }];
    let proposed_outbox = vec![MutationOutboxIntent {
        topic: "engine.projection.rebuild".to_string(),
        key: "batch-1".to_string(),
        payload,
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
        wtx.commit().expect("commit clear");
    }
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
