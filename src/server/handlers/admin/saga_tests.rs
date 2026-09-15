//! Authenticated saga key and async scope isolation.
use super::*;
use crate::server::auth::VerifiedRequestContext;
use sha2::Digest;

fn authority(actor: &str, tenant: &str, key: &str) -> CarrierAuthority {
    let mut claims = crate::acl::RequestContextClaims::default();
    claims.principal = actor.to_string();
    claims.agent_id = actor.to_string();
    claims.tenant = tenant.to_string();
    claims.audience = "epistemic-graph".into();
    claims.policy_version = "test".into();
    CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
        claims,
        key.into(),
    ))
    .unwrap()
}

fn authenticated_saga_id(authority: &CarrierAuthority) -> String {
    authority.namespace("cluster-admin-authenticated", authority.idempotency_key())
}

#[tokio::test]
async fn request_scopes_are_isolated_and_missing_authority_fails_closed() {
    assert!(current_admin_saga_authority().is_err());
    let first = authority("alice", "a", "key-a");
    let second = authority("bob", "b", "key-b");
    let (a, b) = tokio::join!(
        scope_admin_saga_authority(first, async {
            tokio::task::yield_now().await;
            assert!(
                tokio::spawn(async { current_admin_saga_authority().is_err() })
                    .await
                    .unwrap()
            );
            current_admin_saga_authority()
                .unwrap()
                .idempotency_key()
                .to_string()
        }),
        scope_admin_saga_authority(second, async {
            tokio::task::yield_now().await;
            current_admin_saga_authority()
                .unwrap()
                .idempotency_key()
                .to_string()
        }),
    );
    assert_eq!((a.as_str(), b.as_str()), ("key-a", "key-b"));
    assert!(current_admin_saga_authority().is_err());
}

#[test]
fn saga_keys_separate_tenant_principal_and_authenticated_idempotency() {
    let original = authenticated_saga_id(&authority("alice", "a", "key"));
    for different in [
        authority("bob", "a", "key"),
        authority("alice", "b", "key"),
        authority("alice", "a", "other"),
    ] {
        assert_ne!(original, authenticated_saga_id(&different));
    }
    assert_eq!(
        original,
        authenticated_saga_id(&authority("alice", "a", "key"))
    );
}

#[test]
fn admin_replay_rejects_wrong_json_body_and_scalar_arm() {
    let method = Method::Reshard {
        graph: "g".into(),
        to_shard: 1,
    };
    let contract = AdminSagaResultContract::for_method(&method).unwrap();
    assert!(contract
        .validate(&crate::protocol::ResultPayload::Json(
            serde_json::json!({"unrelated": true}),
        ))
        .is_err());
    assert!(contract
        .validate(&crate::protocol::ResultPayload::Bool(true))
        .is_err());
}

#[test]
fn result_contract_mapping_and_private_sagas_fail_closed() {
    let contract = AdminSagaResultContract::for_method(&Method::Reshard {
        graph: "g".into(),
        to_shard: 1,
    })
    .unwrap();
    assert_eq!(contract, AdminSagaResultContract::ShardReshardReport);
    assert_eq!(
        AdminSagaResultContract::for_private_event("sparql_http_recovery_plan_v1").unwrap(),
        AdminSagaResultContract::SparqlRecoveryOutcome
    );
    for public in [
        AdminSagaResultContract::Bool,
        AdminSagaResultContract::Count,
        AdminSagaResultContract::Text,
        AdminSagaResultContract::ShardReshardReport,
        AdminSagaResultContract::RebalanceExecution,
        AdminSagaResultContract::RestoreReceipt,
        AdminSagaResultContract::MultiGraphBatchReport,
        AdminSagaResultContract::ChannelCreated,
        AdminSagaResultContract::ChannelDeparture,
        AdminSagaResultContract::BeliefMaterialization,
    ] {
        assert_eq!(
            AdminSagaResultContract::for_public_discriminator(public.public_discriminator())
                .unwrap(),
            public
        );
    }
    assert!(AdminSagaResultContract::for_public_discriminator("unknown").is_err());
    assert!(AdminSagaResultContract::for_private_event("unregistered-private-saga").is_err());
}

fn sparql_result(operations: u64) -> crate::protocol::ResultPayload {
    crate::protocol::ResultPayload::Json(serde_json::json!({
        "operations": operations,
        "inserted": 2,
        "deleted": 1,
        "updated_graphs": 1,
        "created_graphs": 0,
    }))
}

fn reshard_report(graph: &str) -> eg_types::result_contract::cluster::ShardReshardReport {
    eg_types::result_contract::cluster::ShardReshardReport {
        graph: graph.into(),
        from_shard: 0,
        to_shard: 1,
        nodes: 3,
        edges: 2,
        ledger: 1,
        semantic: 1,
        audit: 1,
        delta_nodes: 0,
        delta_edges: 0,
        no_op: false,
    }
}

fn sealed_private_payload(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    plaintext: &[u8],
) -> (String, Vec<u8>) {
    let digest = hex::encode(sha2::Sha256::digest(plaintext));
    let sealed = backend
        .transaction_recovery_cipher()
        .expect("private recovery fixture requires the canonical recovery cipher")
        .seal(plaintext);
    (digest, sealed)
}

fn opaque_recovery_batch(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch_id: &str,
    event_type: &str,
    payload_digest: &str,
) -> MutationBatch {
    let expected = eg_transaction::version(&backend.admin_mutations_read().unwrap()).unwrap();
    crate::server::mutation_batch::compile_opaque_digest(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: 90,
            attempt_nonce: None,
            principal: Some("contract-test-actor"),
            tenant: "native",
            graph: "cluster-admin",
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: 90,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        payload_digest,
        MutationSurface::Transaction,
        DurabilityDomain::ControlPlane,
        event_type,
    )
    .unwrap()
}

fn legacy_public_batch(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch_id: &str,
    method: &Method,
) -> MutationBatch {
    let expected = eg_transaction::version(&backend.admin_mutations_read().unwrap()).unwrap();
    crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: 90,
            attempt_nonce: None,
            principal: Some("contract-test-actor"),
            tenant: "native",
            graph: "cluster-admin",
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: 90,
            default_surface: MutationSurface::Other,
            authoritative_state: None,
        },
        method,
        MutationSurface::Other,
        DurabilityDomain::MultiGraph,
        "cluster_admin_operation",
    )
    .unwrap()
}

#[test]
fn recovery_rejects_unknown_prepared_and_committed_events_but_replays_known_private() {
    let _env_read_lock = crate::crypto::provisioned_test_env_read_lock_blocking();
    let directory = tempfile::Builder::new()
        .prefix("eg-admin-saga-recovery-contract-")
        .tempdir_in("/var/tmp")
        .unwrap();
    let backend = crate::server::persistence::redb_backend::RedbBackend::open_with_shards(
        directory.path().to_string_lossy().into_owned(),
        16,
        1,
    )
    .unwrap();

    let (prepared_digest, prepared_payload) =
        sealed_private_payload(&backend, b"unknown prepared recovery plan");
    let prepared = opaque_recovery_batch(
        &backend,
        "unknown-prepared",
        "unknown-event",
        &prepared_digest,
    );
    assert!(matches!(
        backend
            .admin_saga_step(&prepared, 90, Some(&prepared_payload))
            .unwrap(),
        eg_transaction::SagaBegin::Execute
    ));
    let Err(error) =
        resume_named_admin_saga(&backend, "unknown-prepared", Some("contract-test-actor"))
    else {
        panic!("unknown Prepared event recovered");
    };
    assert!(error.contains("no declared result contract"));

    let (committed_digest, committed_payload) =
        sealed_private_payload(&backend, b"unknown committed recovery plan");
    let committed = opaque_recovery_batch(
        &backend,
        "unknown-committed",
        "unknown-event",
        &committed_digest,
    );
    assert!(matches!(
        backend
            .admin_saga_step(&committed, 91, Some(&committed_payload))
            .unwrap(),
        eg_transaction::SagaBegin::Execute
    ));
    let encoded = rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true)).unwrap();
    backend.admin_saga_end(&committed, encoded, 91).unwrap();
    let Err(error) =
        resume_named_admin_saga(&backend, "unknown-committed", Some("contract-test-actor"))
    else {
        panic!("unknown Committed event recovered");
    };
    assert!(error.contains("no declared result contract"));

    let (known_digest, known_payload) =
        sealed_private_payload(&backend, b"known private recovery plan");
    let known = begin_named_admin_saga_with_private_payload_and_nonce(
        &backend,
        92,
        Some("contract-test-actor"),
        None,
        AdminSagaPayload {
            domain: DurabilityDomain::ControlPlane,
            batch_id: "known-private",
            event_type: "transaction_recovery_plan",
            payload_digest: &known_digest,
            encrypted_payload: &known_payload,
        },
    )
    .unwrap();
    finish_admin_saga(
        &backend,
        known.batch,
        known.created_at_ms,
        crate::protocol::ResultPayload::Bool(true),
    )
    .unwrap();
    let replay = resume_named_admin_saga(&backend, "known-private", Some("contract-test-actor"))
        .unwrap()
        .unwrap();
    assert!(matches!(
        replay.replayed,
        Some(crate::protocol::ResultPayload::Bool(true))
    ));
}

#[test]
fn sparql_recovery_rejects_untyped_results_while_prepared_and_replays_exact_outcomes() {
    let _env_read_lock = crate::crypto::provisioned_test_env_read_lock_blocking();
    let directory = tempfile::Builder::new()
        .prefix("eg-admin-sparql-recovery-contract-")
        .tempdir_in("/var/tmp")
        .unwrap();
    let backend = crate::server::persistence::redb_backend::RedbBackend::open_with_shards(
        directory.path().to_string_lossy().into_owned(),
        16,
        1,
    )
    .unwrap();
    let (success_digest, success_payload) =
        sealed_private_payload(&backend, b"SPARQL success recovery plan");
    let success = begin_named_admin_saga_with_private_payload_and_nonce(
        &backend,
        100,
        Some("contract-test-actor"),
        None,
        AdminSagaPayload {
            domain: DurabilityDomain::MultiGraph,
            batch_id: "sparql-success",
            event_type: "sparql_http_recovery_plan_v1",
            payload_digest: &success_digest,
            encrypted_payload: &success_payload,
        },
    )
    .unwrap();
    for invalid in [
        crate::protocol::ResultPayload::Json(serde_json::json!({"unrelated": true})),
        crate::protocol::ResultPayload::Json(serde_json::json!({
            "operations": 1,
            "inserted": 2,
            "deleted": 1,
            "updated_graphs": 1,
            "created_graphs": 0,
            "extra": true,
        })),
        crate::protocol::ResultPayload::Json(serde_json::json!({
            "outcome": "compensated",
            "updated_graphs": 1,
            "created_graphs": 0,
        })),
        crate::protocol::ResultPayload::Bool(true),
    ] {
        let recovered =
            resume_named_admin_saga(&backend, "sparql-success", Some("contract-test-actor"))
                .unwrap()
                .unwrap();
        assert!(recovered.prepared);
        assert!(
            finish_admin_saga(&backend, recovered.batch, recovered.created_at_ms, invalid,)
                .is_err()
        );
        let record =
            eg_transaction::read_ledger(&backend.admin_mutations_read().unwrap(), "sparql-success")
                .unwrap()
                .unwrap();
        assert_eq!(
            record.status,
            crate::mutation_batch::MutationBatchStatus::Prepared
        );
        assert!(record.result_msgpack.is_none());
    }
    finish_admin_saga(
        &backend,
        success.batch,
        success.created_at_ms,
        sparql_result(1),
    )
    .unwrap();
    let replay = resume_named_admin_saga(&backend, "sparql-success", Some("contract-test-actor"))
        .unwrap()
        .unwrap();
    let crate::protocol::ResultPayload::Json(body) = replay.replayed.unwrap() else {
        panic!("typed SPARQL success replay lost its JSON result");
    };
    let crate::protocol::ResultPayload::Json(expected) = sparql_result(1) else {
        unreachable!();
    };
    assert_eq!(body, expected);

    let (compensated_digest, compensated_payload) =
        sealed_private_payload(&backend, b"SPARQL compensated recovery plan");
    let compensated = begin_named_admin_saga_with_private_payload_and_nonce(
        &backend,
        101,
        Some("contract-test-actor"),
        None,
        AdminSagaPayload {
            domain: DurabilityDomain::MultiGraph,
            batch_id: "sparql-compensated",
            event_type: "sparql_http_recovery_plan_v1",
            payload_digest: &compensated_digest,
            encrypted_payload: &compensated_payload,
        },
    )
    .unwrap();
    let outcome = crate::protocol::ResultPayload::Json(serde_json::json!({
        "outcome": "compensated",
        "updated_graphs": 0,
        "created_graphs": 0,
    }));
    finish_admin_saga(
        &backend,
        compensated.batch,
        compensated.created_at_ms,
        outcome.clone(),
    )
    .unwrap();
    let replay =
        resume_named_admin_saga(&backend, "sparql-compensated", Some("contract-test-actor"))
            .unwrap()
            .unwrap();
    let crate::protocol::ResultPayload::Json(body) = replay.replayed.unwrap() else {
        panic!("typed SPARQL compensation replay lost its JSON result");
    };
    let crate::protocol::ResultPayload::Json(expected) = outcome else {
        unreachable!();
    };
    assert_eq!(body, expected);
}

#[test]
fn method_bearing_retry_recovers_legacy_public_sagas_but_direct_resume_refuses() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock_blocking();
    let directory = tempfile::Builder::new()
        .prefix("eg-admin-public-legacy-contract-")
        .tempdir_in("/var/tmp")
        .unwrap();
    let backend = crate::server::persistence::redb_backend::RedbBackend::open_with_shards(
        directory.path().to_string_lossy().into_owned(),
        16,
        1,
    )
    .unwrap();
    let method = Method::Reshard {
        graph: "legacy-public".into(),
        to_shard: 1,
    };
    let prepared = legacy_public_batch(&backend, "legacy-public-prepared", &method);
    assert!(matches!(
        backend.admin_saga_step(&prepared, 102, None).unwrap(),
        eg_transaction::SagaBegin::Execute
    ));
    let Err(error) = resume_named_admin_saga(
        &backend,
        "legacy-public-prepared",
        Some("contract-test-actor"),
    ) else {
        panic!("methodless recovery accepted a legacy public Prepared saga");
    };
    assert!(error.contains("missing its result-contract discriminator"));
    let recovered = begin_named_admin_saga_with_nonce(
        &backend,
        103,
        Some("contract-test-actor"),
        &method,
        DurabilityDomain::MultiGraph,
        "legacy-public-prepared",
        None,
    )
    .unwrap();
    assert!(recovered.prepared);
    let Method::ApplyMutation { event_type, .. } = &recovered.batch.operations[0].method else {
        panic!("legacy public recovery lost its opaque operation");
    };
    assert_eq!(event_type, "cluster_admin_operation");
    finish_admin_saga(
        &backend,
        recovered.batch,
        recovered.created_at_ms,
        crate::protocol::ResultPayload::Json(
            serde_json::to_value(reshard_report("legacy-public")).unwrap(),
        ),
    )
    .unwrap();

    let committed = legacy_public_batch(&backend, "legacy-public-committed", &method);
    assert!(matches!(
        backend.admin_saga_step(&committed, 104, None).unwrap(),
        eg_transaction::SagaBegin::Execute
    ));
    let result = crate::protocol::ResultPayload::Json(
        serde_json::to_value(reshard_report("legacy-public")).unwrap(),
    );
    backend
        .admin_saga_end(&committed, rmp_serde::to_vec_named(&result).unwrap(), 104)
        .unwrap();
    let Err(error) = resume_named_admin_saga(
        &backend,
        "legacy-public-committed",
        Some("contract-test-actor"),
    ) else {
        panic!("methodless recovery accepted a legacy public Committed saga");
    };
    assert!(error.contains("missing its result-contract discriminator"));
    let recovered = begin_named_admin_saga_with_nonce(
        &backend,
        105,
        Some("contract-test-actor"),
        &method,
        DurabilityDomain::MultiGraph,
        "legacy-public-committed",
        None,
    )
    .unwrap();
    let crate::protocol::ResultPayload::Json(body) = recovered.replayed.unwrap() else {
        panic!("method-bearing legacy public recovery lost its typed result");
    };
    assert_eq!(
        serde_json::from_value::<eg_types::result_contract::cluster::ShardReshardReport>(body)
            .unwrap(),
        reshard_report("legacy-public")
    );

    let discriminated = begin_named_admin_saga_with_nonce(
        &backend,
        106,
        Some("contract-test-actor"),
        &method,
        DurabilityDomain::MultiGraph,
        "discriminated-public-committed",
        None,
    )
    .unwrap();
    let encoded = rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true)).unwrap();
    backend
        .admin_saga_end(&discriminated.batch, encoded, 106)
        .unwrap();
    let Err(error) = resume_named_admin_saga(
        &backend,
        "discriminated-public-committed",
        Some("contract-test-actor"),
    ) else {
        panic!("public commit replayed a body that violated its persisted result contract");
    };
    assert!(error.contains("requires a JSON body"));
}

#[test]
fn durable_reshard_saga_rejects_wrong_result_then_replays_typed_result() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock_blocking();
    let directory = tempfile::Builder::new()
        .prefix("eg-admin-saga-contract-")
        .tempdir_in("/var/tmp")
        .unwrap();
    let backend = crate::server::persistence::redb_backend::RedbBackend::open_with_shards(
        directory.path().to_string_lossy().into_owned(),
        16,
        1,
    )
    .unwrap();
    let method = Method::Reshard {
        graph: "contract-test".into(),
        to_shard: 1,
    };
    let saga = begin_named_admin_saga_with_nonce(
        &backend,
        1,
        Some("contract-test-actor"),
        &method,
        DurabilityDomain::MultiGraph,
        "reshard-contract-test",
        None,
    )
    .unwrap();
    assert!(!saga.prepared);
    assert_eq!(
        saga.batch.result_contract.unwrap(),
        AdminSagaResultContract::ShardReshardReport
    );
    let Method::ApplyMutation { event_type, .. } = &saga.batch.operations[0].method else {
        panic!("admin saga did not retain its opaque durable operation");
    };
    assert_eq!(
        event_type,
        "cluster_admin_operation/shard-reshard-report-v1"
    );

    let wrong = finish_admin_saga(
        &backend,
        saga.batch.clone(),
        saga.created_at_ms,
        crate::protocol::ResultPayload::Bool(true),
    );
    assert!(wrong.is_err());
    let prepared = eg_transaction::read_ledger(
        &backend.admin_mutations_read().unwrap(),
        "reshard-contract-test",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        prepared.status,
        crate::mutation_batch::MutationBatchStatus::Prepared
    );
    assert!(prepared.result_msgpack.is_none());
    let recovered = resume_named_admin_saga(
        &backend,
        "reshard-contract-test",
        Some("contract-test-actor"),
    )
    .unwrap()
    .unwrap();
    assert!(recovered.prepared);
    assert!(finish_admin_saga(
        &backend,
        recovered.batch.clone(),
        recovered.created_at_ms,
        crate::protocol::ResultPayload::Bool(true),
    )
    .unwrap_err()
    .contains("requires a JSON body"));

    let report = reshard_report("contract-test");
    let result = crate::protocol::ResultPayload::Json(serde_json::to_value(&report).unwrap());
    let committed = finish_admin_saga(
        &backend,
        recovered.batch,
        recovered.created_at_ms,
        result.clone(),
    )
    .unwrap();
    assert!(matches!(committed, crate::protocol::ResultPayload::Json(_)));

    let replay = begin_named_admin_saga_with_nonce(
        &backend,
        2,
        Some("contract-test-actor"),
        &method,
        DurabilityDomain::MultiGraph,
        "reshard-contract-test",
        None,
    )
    .unwrap();
    let crate::protocol::ResultPayload::Json(body) = replay.replayed.unwrap() else {
        panic!("reshard replay lost its typed JSON result");
    };
    assert_eq!(
        serde_json::from_value::<eg_types::result_contract::cluster::ShardReshardReport>(body)
            .unwrap(),
        report
    );
}
