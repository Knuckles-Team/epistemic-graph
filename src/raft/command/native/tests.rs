use super::*;
use crate::raft::ReplicatedMutation;

fn work_item_mutations() -> Vec<Method> {
    use crate::epistemic_operations::{
        ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ResourceCapacity,
        ResourceHostUpdateRequest, ResourceHostUpdateRequestSchemaVersion,
        ResourceHostUpdateRequestTargetKind, ResourceRequirement, ResourceReservationRequest,
        ResourceReservationRequestSchemaVersion, ResourceReservationRequestTargetKind,
    };
    use crate::epistemic_operations_ext::{
        CasWorkItemMetadataRequest, CasWorkItemMetadataRequestSchemaVersion,
        WorkItemClaimCapabilityMintRequest, WorkItemClaimCapabilityRequestSchemaVersion,
    };

    let requirement = ResourceRequirement {
        cpu_weight: 2,
        memory_mib: 128,
        disk_mib: 256,
        process_slots: 1,
    };
    let reservation = ResourceReservationRequest {
        schema_version: ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: "tenant-ref".to_string(),
        work_item_id: "work-item".to_string(),
        owner_id: "worker".to_string(),
        fence: "fence".to_string(),
        lease_epoch: 3,
        fencing_token: 7,
        attempt: 1,
        reservation_id: "reservation".to_string(),
        input_fingerprint: "input-fingerprint".to_string(),
        profile_name: "cpu-small".to_string(),
        profile_version: "1".to_string(),
        host_ref: "host".to_string(),
        requirement,
        target_kind: ResourceReservationRequestTargetKind::Local,
        target_alias: None,
        repository_id: "repository".to_string(),
        branch: "main".to_string(),
        concurrency_key: "tenant:repository".to_string(),
        concurrency_limit: Some(2),
        repository_exclusive: false,
        branch_exclusive: true,
        required_labels: vec!["linux".to_string()],
        anti_affinity: vec!["gpu".to_string()],
        fairness_group: "default".to_string(),
        fairness_cost: 4,
        disk_low_watermark_mib: Some(512),
        disk_high_watermark_mib: Some(1024),
        disk_policy_key: "default".to_string(),
        reserved_at_ms: 10,
        expires_at_ms: 20,
        idempotency_key: "reservation-idempotency".to_string(),
        now_ms: 10,
        expected_host_revision: Some(4),
        expected_lifecycle_revision: Some(8),
    };
    let host = ResourceHostUpdateRequest {
        schema_version: ResourceHostUpdateRequestSchemaVersion::V1,
        tenant_ref: "tenant-ref".to_string(),
        host_ref: "host".to_string(),
        revision: 5,
        capacity: ResourceCapacity {
            cpu_weight: 16,
            memory_mib: 4096,
            disk_mib: 8192,
            process_slots: 8,
        },
        observed: ResourceCapacity {
            cpu_weight: 12,
            memory_mib: 3072,
            disk_mib: 6144,
            process_slots: 6,
        },
        heartbeat_at_ms: 10,
        heartbeat_ttl_ms: 100,
        now_ms: 10,
        draining: false,
        quarantined: false,
        labels: vec!["linux".to_string()],
        target_kind: ResourceHostUpdateRequestTargetKind::Local,
        target_alias: None,
        disk_used_mib: 2048,
        disk_capacity_mib: 8192,
    };

    vec![
        Method::ClaimWorkItem {
            request: ClaimWorkItemRequest {
                schema_version: ClaimWorkItemRequestSchemaVersion::V1,
                tenant_ref: "tenant-ref".to_string(),
                work_item_id: Some("work-item".to_string()),
                queue_ref: Some("queue".to_string()),
                resource_class: Some("cpu-small".to_string()),
                fairness_group: Some("default".to_string()),
                worker_ref: "worker".to_string(),
                now_ms: 10,
                lease_ms: 100,
                max_tenant_in_flight: 2,
            },
        },
        Method::MintWorkItemClaimCapability {
            request: WorkItemClaimCapabilityMintRequest {
                schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                work_item_id: "work-item".to_string(),
            },
        },
        Method::RenewWorkItemLease {
            tenant: "tenant-ref".to_string(),
            work_item_id: "work-item".to_string(),
            worker_id: "worker".to_string(),
            lease_epoch: 3,
            fencing_token: 7,
            now_ms: 20,
            lease_ms: 100,
        },
        Method::CommitWorkItemResult {
            tenant: "tenant-ref".to_string(),
            work_item_id: "work-item".to_string(),
            worker_id: "worker".to_string(),
            lease_epoch: 3,
            fencing_token: 7,
            idempotency_key: "result-idempotency".to_string(),
            outcome: "succeeded".to_string(),
            result_ref: Some("result-ref".to_string()),
            outcome_extension: None,
            error_ref: None,
            retryable: false,
            now_ms: 30,
        },
        Method::CancelWorkItem {
            tenant: "tenant-ref".to_string(),
            work_item_id: "work-item".to_string(),
            idempotency_key: "cancel-idempotency".to_string(),
            reason_ref: Some("reason-ref".to_string()),
            now_ms: 40,
        },
        Method::DeferWorkItem {
            tenant: "tenant-ref".to_string(),
            work_item_id: "work-item".to_string(),
            worker_id: "worker".to_string(),
            lease_epoch: 3,
            fencing_token: 7,
            idempotency_key: "defer-idempotency".to_string(),
            next_retry_at_ms: 80,
            reason_ref: Some("barrier".to_string()),
            now_ms: 50,
        },
        Method::CasWorkItemMetadata {
            request: CasWorkItemMetadataRequest {
                schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
                tenant_ref: "tenant-ref".to_string(),
                work_item_id: "work-item".to_string(),
                expected_lease: None,
                expected_status: vec!["leased".to_string(), "running".to_string()],
                expected_checkpoint_id: None,
                set_checkpoint_id: Some("checkpoint:1".to_string()),
                expected_metadata_msgpack: None,
                set_metadata_msgpack: None,
                expected_prio_bucket: None,
                set_prio_bucket: None,
                now_ms: 60,
            },
        },
        Method::ReserveWorkItemResources {
            request: reservation.clone(),
        },
        Method::ReleaseWorkItemResources {
            request: reservation.clone(),
        },
        Method::ReclaimWorkItemResources {
            request: reservation,
        },
        Method::UpdateResourceHost { request: host },
    ]
}

#[test]
fn work_item_mutations_round_trip_through_sealed_native_command() {
    for method in work_item_mutations() {
        let expected = rmp_serde::to_vec_named(&method).unwrap();
        let command =
            NativeMutationCommand::from_public_method(method.clone(), "cluster-work-item-key")
                .expect("work-item mutation has a native consensus command");
        assert!(matches!(&command, NativeMutationCommand::WorkItem { .. }));
        assert_eq!(command.domain(), Some(NativeMutationDomain::WorkItem));

        let opened = command
            .open_public_method("cluster-work-item-key")
            .unwrap()
            .expect("sealed work-item method opens");
        assert_eq!(rmp_serde::to_vec_named(&opened).unwrap(), expected);

        let replicated = ReplicatedMutation::native_method(method, "cluster-work-item-key")
            .expect("work-item mutation enters ReplicatedMutation::Native");
        assert!(replicated
            .open_graph("cluster-work-item-key")
            .unwrap()
            .is_none());
    }
}

#[test]
fn work_item_native_inventory_excludes_read_only_reservation_queries() {
    for method in [
        "ClaimWorkItem",
        "MintWorkItemClaimCapability",
        "RenewWorkItemLease",
        "CommitWorkItemResult",
        "CancelWorkItem",
        "DeferWorkItem",
        "CasWorkItemMetadata",
        "ReserveWorkItemResources",
        "ReleaseWorkItemResources",
        "ReclaimWorkItemResources",
        "UpdateResourceHost",
    ] {
        assert!(
            NATIVE_CONSENSUS_METHODS.contains(&method),
            "missing WorkItem native inventory entry: {method}"
        );
    }
    for query in ["QueryWorkItemReservation", "ResourceReservationStatus"] {
        assert!(
            !NATIVE_CONSENSUS_METHODS.contains(&query),
            "read-only reservation query must not enter consensus: {query}"
        );
    }
}

#[test]
fn node_info_is_typed_internal_and_sealed() {
    let info = crate::server::persistence::node_info_store::NodeInfo {
        cluster_id: "cluster-authority".to_string(),
        node_id: 7,
        member_identity: crate::server::persistence::node_info_store::member_identity_for(
            "cluster-authority",
            7,
        ),
        raft_addr: "127.0.0.1:9100".to_string(),
        advertised_client_addr: "tcp://127.0.0.1:9101".to_string(),
        tls_server_name: None,
        certificate_id: None,
        certificate_rotation_epoch: 0,
        certificate_not_before_ms: None,
        certificate_not_after_ms: None,
    };
    let command = NativeMutationCommand::node_info(&info, "cluster-node-info-key").unwrap();
    assert_eq!(command.domain(), Some(NativeMutationDomain::ClusterAdmin));
    assert!(command
        .open_public_method("cluster-node-info-key")
        .unwrap()
        .is_none());
    assert_eq!(
        command.open_node_info("cluster-node-info-key").unwrap(),
        info
    );
    let wire = rmp_serde::to_vec_named(&command).unwrap();
    assert!(!wire
        .windows(b"tcp://127.0.0.1:9101".len())
        .any(|window| window == b"tcp://127.0.0.1:9101"));
    assert!(command.open_node_info("wrong-key").is_err());
}

fn assert_native_round_trip(method: Method, expected_domain: NativeMutationDomain) {
    let expected = rmp_serde::to_vec_named(&method).unwrap();
    let command = NativeMutationCommand::from_public_method(method, "native-domain-test-key")
        .expect("representative method has a native command");
    assert_eq!(command.domain(), Some(expected_domain));
    let opened = command
        .open_public_method("native-domain-test-key")
        .unwrap()
        .expect("public native method opens");
    assert_eq!(rmp_serde::to_vec_named(&opened).unwrap(), expected);
}

#[test]
fn native_catalog_is_complete_unique_and_has_domain_representatives() {
    let unique = NATIVE_CONSENSUS_METHODS
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    // `NodeInfoUpsert` moved behind the sealed native command envelope in
    // 7469acff; it is intentionally absent from the public method catalog.
    assert_eq!(NATIVE_CONSENSUS_METHODS.len(), 100);
    assert_eq!(unique.len(), NATIVE_CONSENSUS_METHODS.len());
    assert!(unique.iter().all(|name| !name.is_empty()));

    assert_native_round_trip(Method::ClearLedger, NativeMutationDomain::GraphState);
    #[cfg(feature = "shacl")]
    assert_native_round_trip(
        Method::GraphSchema {
            op: Box::new(eg_types::graph_schema::GraphSchemaOp::Detach {
                source_id: "admin:policy".to_string(),
                if_composed_digest: None,
            }),
        },
        NativeMutationDomain::GraphState,
    );
    assert_native_round_trip(
        Method::Rollback {
            txn_id: "txn".to_string(),
        },
        NativeMutationDomain::Transaction,
    );
    assert_native_round_trip(
        work_item_mutations().remove(0),
        NativeMutationDomain::WorkItem,
    );
    #[cfg(feature = "blob")]
    assert_native_round_trip(Method::BlobGc, NativeMutationDomain::Blob);
    #[cfg(feature = "kv")]
    assert_native_round_trip(
        Method::KvDelete {
            namespace: "namespace".to_string(),
            key: "key".to_string(),
        },
        NativeMutationDomain::KeyValue,
    );
    #[cfg(feature = "tsdb")]
    assert_native_round_trip(
        Method::TsDeleteSeries {
            series_id: "series".to_string(),
        },
        NativeMutationDomain::TimeSeries,
    );
    #[cfg(feature = "jobs")]
    assert_native_round_trip(
        Method::AnalyticsJob {
            op: eg_types::jobs::JobOp::Status {
                job_id: "job".to_string(),
            },
        },
        NativeMutationDomain::AnalyticsJob,
    );
    #[cfg(feature = "sqlite-file")]
    assert_native_round_trip(
        Method::ImportSqliteFile {
            path: "catalog.db".to_string(),
        },
        NativeMutationDomain::SqliteCatalog,
    );
    assert_native_round_trip(
        Method::CloseChannel {
            channel_id: "channel".to_string(),
            summary_embedding: None,
            topic_metadata: None,
        },
        NativeMutationDomain::SessionControl,
    );
    assert_native_round_trip(
        Method::RbacAdmin {
            op: crate::acl::RbacAdminOp::List,
        },
        NativeMutationDomain::Identity,
    );
    assert_native_round_trip(
        Method::Restore {
            source: "backup".to_string(),
            target_shards: 1,
        },
        NativeMutationDomain::ClusterAdmin,
    );
    assert_native_round_trip(
        Method::DeleteGraph {
            graph_name: "graph".to_string(),
        },
        NativeMutationDomain::GraphLifecycle,
    );
    assert_native_round_trip(
        Method::ApplyMultisigMutation {
            signatures: vec!["signature".to_string()],
            threshold: 1,
            mutation_type: "mutation".to_string(),
            query: "query".to_string(),
        },
        NativeMutationDomain::Multisig,
    );
    #[cfg(feature = "statechart")]
    assert_native_round_trip(
        Method::Statechart {
            op: eg_types::statechart::StatechartOp::List { def_id: None },
        },
        NativeMutationDomain::Statechart,
    );
}

#[test]
fn cypher_write_is_native_and_read_is_not() {
    let write = Method::CypherQuery {
        query: "CREATE (:Node)".to_string(),
        mode: crate::protocol::CypherMode::Write,
    };
    let read = Method::CypherQuery {
        query: "MATCH (n) RETURN n".to_string(),
        mode: crate::protocol::CypherMode::Read,
    };

    assert_eq!(
        native_domain(&write),
        Some(NativeMutationDomain::GraphState)
    );
    assert_eq!(native_domain(&read), None);
    assert!(NativeMutationCommand::from_public_method(write, "cypher-key").is_ok());
    assert!(NativeMutationCommand::from_public_method(read, "cypher-key").is_err());
}

#[test]
fn cross_domain_ciphertext_substitution_is_rejected() {
    let graph =
        NativeMutationCommand::from_public_method(Method::ClearLedger, "domain-key").unwrap();
    let sealed_method = sealed_method(&graph).unwrap().clone();
    let substituted = NativeMutationCommand::Transaction { sealed_method };

    assert_eq!(
        substituted.open_public_method("domain-key").unwrap_err(),
        "native Raft command method is outside its declared domain"
    );
}

#[derive(Serialize)]
struct SealedWire<'a> {
    #[serde(with = "serde_bytes")]
    ciphertext: &'a [u8],
    plaintext_sha256: &'a str,
}

fn sealed_from_wire(ciphertext: &[u8], digest: &str) -> SealedNativeMethod {
    rmp_serde::from_slice(
        &rmp_serde::to_vec_named(&SealedWire {
            ciphertext,
            plaintext_sha256: digest,
        })
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn native_envelopes_reject_empty_unsealed_malformed_oversize_and_wrong_key() {
    let digest = "a".repeat(64);
    assert!(sealed_from_wire(&[], &digest).validate_shape().is_err());
    assert!(sealed_from_wire(&[1, 2, 3], &digest)
        .validate_shape()
        .is_err());
    assert!(sealed_from_wire(&[0xE6; 14], "NOT-A-DIGEST")
        .validate_shape()
        .is_err());
    assert!(!super::sealed::native_envelope_shape_is_valid(
        super::sealed::MAX_REPLICATED_COMMAND_PAYLOAD_BYTES
            + super::sealed::MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES
            + 1,
        true,
        &digest,
    ));

    let command =
        NativeMutationCommand::from_public_method(Method::ClearLedger, "correct-key").unwrap();
    assert!(command.open_public_method("wrong-key").is_err());
}

fn coordinator_id() -> String {
    format!("coordinator:{}", "a".repeat(64))
}

#[test]
fn participant_phase_and_coordinator_shapes_fail_closed() {
    let plan = b"plan";
    for phase in [
        TransactionParticipantPhase::Prepare,
        TransactionParticipantPhase::Commit,
    ] {
        let command = NativeMutationCommand::transaction_participant(
            phase,
            coordinator_id(),
            1,
            Some(plan),
            "participant-key",
        )
        .unwrap();
        assert_eq!(
            command.open_transaction_plan("participant-key").unwrap(),
            Some(plan.to_vec())
        );
    }
    let abort = NativeMutationCommand::transaction_participant(
        TransactionParticipantPhase::Abort,
        coordinator_id(),
        1,
        None,
        "participant-key",
    )
    .unwrap();
    assert_eq!(
        abort.open_transaction_plan("participant-key").unwrap(),
        None
    );
    assert!(NativeMutationCommand::transaction_participant(
        TransactionParticipantPhase::Prepare,
        coordinator_id(),
        1,
        None,
        "participant-key",
    )
    .is_err());
    assert!(NativeMutationCommand::transaction_participant(
        TransactionParticipantPhase::Abort,
        coordinator_id(),
        1,
        Some(plan),
        "participant-key",
    )
    .is_err());
    assert!(NativeMutationCommand::transaction_participant(
        TransactionParticipantPhase::Prepare,
        "not-an-opaque-scope".to_string(),
        1,
        Some(plan),
        "participant-key",
    )
    .is_err());
}

#[cfg(feature = "jobs")]
#[test]
fn job_publication_payloads_authenticate_in_the_jobs_domain() {
    let commit =
        NativeMutationCommand::job_publication_commit(coordinator_id(), b"plan", "jobs-key")
            .unwrap();
    let finalize =
        NativeMutationCommand::job_publication_finalize(coordinator_id(), b"receipt", "jobs-key")
            .unwrap();

    for (command, expected) in [
        (commit, b"plan".as_slice()),
        (finalize, b"receipt".as_slice()),
    ] {
        assert_eq!(command.domain(), Some(NativeMutationDomain::AnalyticsJob));
        assert_eq!(
            command.open_job_publication_payload("jobs-key").unwrap(),
            expected
        );
        assert!(command.open_job_publication_payload("wrong-key").is_err());
    }
}

#[test]
fn representative_command_msgpack_shape_is_byte_stable() {
    let command = NativeMutationCommand::TransactionDecision {
        coordinator_id: "c".to_string(),
        commit: true,
    };
    assert_eq!(
        rmp_serde::to_vec_named(&command).unwrap(),
        vec![
            129, 180, 116, 114, 97, 110, 115, 97, 99, 116, 105, 111, 110, 95, 100, 101, 99, 105,
            115, 105, 111, 110, 130, 174, 99, 111, 111, 114, 100, 105, 110, 97, 116, 111, 114, 95,
            105, 100, 161, 99, 166, 99, 111, 109, 109, 105, 116, 195,
        ]
    );
}

/// The three bounds are compile-time constants, so this is checked at compile
/// time rather than at test time: a build that violates it does not link, and
/// there is no run in which the assertion could be skipped.
const _: () = assert!(
    super::sealed::MAX_REPLICATED_COMMAND_PAYLOAD_BYTES
        + super::sealed::MAX_SEALED_NATIVE_COMMAND_OVERHEAD_BYTES
        < crate::raft::network::MAX_RAFT_PAYLOAD_BYTES
);
