//! Parent-only phases must authenticate the durable tenant even without a plan.

use super::*;
use crate::server::persistence::backup::EnvVarGuard;
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;

fn carrier(tenant: &str) -> CarrierAuthority {
    CarrierAuthority::from_verified(&VerifiedRequestContext::verified_for_test_in_tenant(
        "same-principal",
        tenant,
    ))
    .unwrap()
}

fn phase_authority(carrier: &CarrierAuthority) -> crate::raft::RaftMutationContext {
    crate::raft::RaftMutationContext::from_verified_request(
        "phase-authority".into(),
        1,
        None,
        carrier.tenant_scope(),
        carrier.actor_scope().to_string(),
        false,
        0,
        crate::raft::RaftMutationTiming {
            fencing_token: None,
            created_at_ms: 1,
        },
    )
    .unwrap()
}

async fn check_parent_only_phases(
    state: &Arc<RwLock<ServerState>>,
    redb: &RedbBackend,
    parent_id: &str,
    owner: &crate::raft::RaftMutationContext,
    foreign: &crate::raft::RaftMutationContext,
) {
    assert!(redb.xshard_prepare_get(parent_id, 7).unwrap().is_none());
    let denied = apply_consensus_participant_abort(state, foreign, parent_id, 7)
        .await
        .unwrap_err();
    assert!(denied.contains("tenant scope"), "{denied}");
    let denied = apply_consensus_transaction_decision(state, parent_id, foreign, false)
        .await
        .unwrap_err();
    assert!(denied.contains("tenant scope"), "{denied}");
    assert!(redb.xshard_decision_get(parent_id).unwrap().is_none());
    assert!(
        apply_consensus_participant_abort(state, owner, parent_id, 7)
            .await
            .unwrap()
    );
    assert!(
        !apply_consensus_transaction_decision(state, parent_id, owner, false)
            .await
            .unwrap()
    );
    let denied = apply_consensus_transaction_finalize(state, parent_id, foreign, false)
        .await
        .unwrap_err();
    assert!(denied.contains("tenant scope"), "{denied}");
    assert_eq!(redb.xshard_decision_get(parent_id).unwrap(), Some(false));
    assert!(
        !apply_consensus_transaction_finalize(state, parent_id, owner, false)
            .await
            .unwrap()
    );
    assert!(
        eg_transaction::read_private_payload(&redb.admin_mutations_read().unwrap(), parent_id)
            .unwrap()
            .is_none()
    );
    // The tenant check still works after terminalization erases the private plan.
    assert!(
        apply_consensus_participant_abort(state, foreign, parent_id, 7)
            .await
            .unwrap_err()
            .contains("tenant scope")
    );
    assert!(
        apply_consensus_transaction_decision(state, parent_id, foreign, false)
            .await
            .unwrap_err()
            .contains("tenant scope")
    );
    assert!(
        apply_consensus_transaction_finalize(state, parent_id, foreign, false)
            .await
            .unwrap_err()
            .contains("tenant scope")
    );
    assert!(
        apply_consensus_participant_abort(state, owner, parent_id, 7)
            .await
            .unwrap()
    );
    assert!(
        !apply_consensus_transaction_decision(state, parent_id, owner, false)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn parent_only_phases_reject_foreign_tenant_with_same_principal() {
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let _key = EnvVarGuard::set(
        crate::crypto::TXN_RECOVERY_KEY_ENV,
        "parent-tenant-test-key",
    );
    let directory = std::env::temp_dir().join(format!(
        "eg-parent-tenant-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let backend = Arc::new(
        RedbBackend::open_with_shards(directory.to_string_lossy().into_owned(), 64, 1).unwrap(),
    );
    let mut server = ServerState::new_for_test(
        "parent-tenant-test",
        ServerState::test_isolation("same-principal"),
    );
    server.persistence = Some(backend.clone());
    server
        .registry
        .create_graph("delegated-graph", crate::protocol::GraphType::Global, None)
        .unwrap();
    let state = Arc::new(RwLock::new(server));
    let owner = carrier("tenant-a");
    let foreign = carrier("tenant-b");
    assert_eq!(owner.actor_scope(), foreign.actor_scope());
    for key in [None, Some("stable-key")] {
        let mut txn = GraphTxnState::new(
            &crate::graph::GraphCore::new(),
            NewTxnArgs {
                graph: "graph".into(),
                tenant_scope: owner.tenant_scope().into(),
                begin_version: 0,
                isolation: crate::server::txn::IsolationLevel::Snapshot,
                predicate: None,
                agent: owner.owner_scope().into(),
                now_ms: 1,
            },
        );
        txn.write_set.push(Method::RemoveNode {
            node_id: "delayed-node".into(),
        });
        let (receipt, replayed) = begin_txn_receipt(
            Some(backend.clone()),
            1,
            Some(owner.actor_scope()),
            "txn-parent",
            &txn,
            key,
            None,
        )
        .unwrap();
        assert!(replayed.is_none());
        let parent_id = receipt_coordinator_id(&receipt);
        drop(receipt);
        check_parent_only_phases(
            &state,
            &backend,
            &parent_id,
            &phase_authority(&owner),
            &phase_authority(&foreign),
        )
        .await;
        check_delayed_abort_phases(&state, &backend, &parent_id, &txn, &phase_authority(&owner))
            .await;
        check_terminal_success_replay(&state, &backend, &txn, &owner, key).await;
    }
    check_delegated_recovery(&state, &backend).await;
    backend.shutdown();
    state.write().await.persistence = None;
    drop(state);
    drop(backend);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn parent_tenant_binding_rejects_foreign_and_forged_routing_ids() {
    let parent = commit_receipt_id("txn", Some("key"), Some("tenant-a"));
    validate_transaction_receipt_tenant(&parent, "tenant-a").unwrap();
    assert!(validate_transaction_receipt_tenant(&parent, "tenant-b").is_err());
    assert!(validate_transaction_receipt_tenant(&format!("{parent}:changed"), "tenant-a").is_err());
    assert!(
        validate_transaction_receipt_tenant(&transaction_receipt_id("txn"), "tenant-a").is_err()
    );
}

#[test]
fn malformed_operation_ids_fail_even_with_a_matching_tenant_hash() {
    for malformed in [
        "",
        "unrelated:abcd",
        "transaction-receipt:abcd",
        "transaction-receipt-tenant:",
    ] {
        let binding = crate::server::mutation_batch::opaque_coordinator_key(
            "transaction-receipt-scope",
            "tenant-a",
            malformed,
        );
        assert!(
            validate_transaction_receipt_tenant(&format!("{binding}:{malformed}"), "tenant-a")
                .is_err()
        );
    }
}

fn participant_plan(parent_id: &str, txn: &GraphTxnState) -> (u64, Vec<u8>) {
    let participant_id = consensus_participant_id(parent_id, &txn.graph);
    let plan = ConsensusParticipantPlan {
        schema_version: CONSENSUS_TXN_SCHEMA_VERSION,
        coordinator_id: parent_id.into(),
        participant_id,
        graph_name: txn.graph.clone(),
        graph_type: crate::protocol::GraphType::Global,
        group_id: 0,
        placement_epoch: 0,
        fencing_token: None,
        recovery_plan: txn.encode_recovery_plan().unwrap(),
    };
    (participant_id, rmp_serde::to_vec_named(&plan).unwrap())
}

async fn delayed_phases(
    state: &Arc<RwLock<ServerState>>,
    parent: &str,
    txn: &GraphTxnState,
    authority: &crate::raft::RaftMutationContext,
) -> [Result<bool, String>; 2] {
    let (participant_id, bytes) = participant_plan(parent, txn);
    [
        apply_consensus_participant_prepare(
            state,
            0,
            0,
            None,
            authority,
            ConsensusParticipantRef {
                coordinator_id: parent,
                participant_id,
                plan_bytes: &bytes,
            },
        )
        .await,
        apply_consensus_participant_commit(
            state,
            1,
            0,
            authority,
            ConsensusParticipantRef {
                coordinator_id: parent,
                participant_id,
                plan_bytes: &bytes,
            },
        )
        .await,
    ]
}

async fn check_delayed_abort_phases(
    state: &Arc<RwLock<ServerState>>,
    backend: &RedbBackend,
    parent: &str,
    txn: &GraphTxnState,
    authority: &crate::raft::RaftMutationContext,
) {
    for result in delayed_phases(state, parent, txn, authority).await {
        assert!(result.unwrap_err().contains("terminal parent outcome"));
    }
    let (id, _) = participant_plan(parent, txn);
    assert!(backend.xshard_prepare_get(parent, id).unwrap().is_none());
    assert!(!crate::server::txn::consensus_graph_is_prepared(&txn.graph));
}

async fn store_committed_child(
    backend: &Arc<RedbBackend>,
    parent: &str,
    txn: &GraphTxnState,
    actor: &str,
) {
    let (id, _) = participant_plan(parent, txn);
    let child_id = consensus_participant_child_id(parent, id, &txn.graph);
    let core = Arc::new(crate::graph::GraphCore::new());
    let persistence: Arc<dyn PersistenceBackend> = backend.clone();
    let result = crate::server::mutation_batch::commit_internal_graph_methods_with_nonce(
        crate::server::mutation_batch::InternalGraphCommitRequest::new(
            Some(&persistence),
            &core,
            crate::server::mutation_batch::CommitOrigin {
                request_id: 3,
                principal: Some(actor),
            },
            &txn.graph,
            &child_id,
            txn.write_set.clone(),
            &ResultPayload::Bool(true),
        )
        .with_tenant_scope(&txn.tenant_scope),
    )
    .await
    .unwrap();
    assert_eq!(result.record.committing_tenant().unwrap(), txn.tenant_scope);
    let foreign = crate::server::mutation_batch::commit_internal_graph_methods_with_nonce(
        crate::server::mutation_batch::InternalGraphCommitRequest::new(
            Some(&persistence),
            &core,
            crate::server::mutation_batch::CommitOrigin {
                request_id: 4,
                principal: Some(actor),
            },
            &txn.graph,
            &child_id,
            txn.write_set.clone(),
            &ResultPayload::Bool(true),
        )
        .with_tenant_scope("foreign-tenant"),
    )
    .await
    .unwrap_err();
    assert!(foreign.contains("tenant scope"));
    let authority = phase_authority(&carrier("tenant-a"));
    let fname = crate::persist::sanitize(&txn.graph);
    validate_committed_participant_record(&result.record, &child_id, &fname, &authority).unwrap();
    let mut foreign_authority = authority.clone();
    foreign_authority.tenant_scope = carrier("tenant-b").tenant_scope().into();
    assert!(validate_committed_participant_record(
        &result.record,
        &child_id,
        &fname,
        &foreign_authority
    )
    .is_err());
    foreign_authority = authority.clone();
    foreign_authority.principal_fingerprint =
        crate::server::mutation_batch::principal_fingerprint("foreign-principal").unwrap();
    assert!(validate_committed_participant_record(
        &result.record,
        &child_id,
        &fname,
        &foreign_authority
    )
    .is_err());
    let mut prepared = result.record.clone();
    prepared.status = crate::mutation_batch::MutationBatchStatus::Prepared;
    assert!(
        validate_committed_participant_record(&prepared, &child_id, &fname, &authority).is_err()
    );
}

async fn check_terminal_success_replay(
    state: &Arc<RwLock<ServerState>>,
    backend: &Arc<RedbBackend>,
    txn: &GraphTxnState,
    owner: &CarrierAuthority,
    original_key: Option<&str>,
) {
    let key = original_key.map(|_| "terminal-success-key");
    let (receipt, _) = begin_txn_receipt(
        Some(backend.clone()),
        2,
        Some(owner.actor_scope()),
        "terminal-success-txn",
        txn,
        key,
        None,
    )
    .unwrap();
    let parent = receipt_coordinator_id(&receipt);
    finish_txn_receipt(receipt, ResultPayload::Bool(true)).unwrap();
    let authority = phase_authority(owner);
    for result in delayed_phases(state, &parent, txn, &authority).await {
        assert!(result.unwrap_err().contains("no committed child receipt"));
    }
    let (id, bytes) = participant_plan(&parent, txn);
    assert!(backend.xshard_prepare_get(&parent, id).unwrap().is_none());
    // Install the exact historical child proof; replay may now only clean up.
    store_committed_child(backend, &parent, txn, owner.actor_scope()).await;
    backend
        .xshard_prepare_put(&parent, id, bytes)
        .await
        .unwrap();
    let before = backend
        .read_mutation_graph_version(&txn.graph)
        .await
        .unwrap();
    for result in delayed_phases(state, &parent, txn, &authority).await {
        assert!(result.unwrap());
    }
    assert_eq!(
        before,
        backend
            .read_mutation_graph_version(&txn.graph)
            .await
            .unwrap()
    );
    assert!(backend.xshard_prepare_get(&parent, id).unwrap().is_none());
    let mut changed = txn.clone();
    changed.write_set.push(Method::RemoveNode {
        node_id: "different-intent".into(),
    });
    for result in delayed_phases(state, &parent, &changed, &authority).await {
        assert!(result.unwrap_err().contains("parent intent"));
    }
}

fn delegated_carrier(principal: &str) -> CarrierAuthority {
    let base = VerifiedRequestContext::verified_for_test_in_tenant("same-principal", "tenant-a");
    let mut claims = base.claims().clone();
    claims.principal = principal.into();
    CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
        claims,
        "delegated-key".into(),
    ))
    .unwrap()
}

async fn check_delegated_recovery(state: &Arc<RwLock<ServerState>>, backend: &Arc<RedbBackend>) {
    let owner = delegated_carrier("authenticated-user");
    let foreign = delegated_carrier("foreign-user");
    let core = state
        .read()
        .await
        .registry
        .get("delegated-graph")
        .unwrap()
        .core
        .clone();
    let mut txn = GraphTxnState::new(
        &core,
        NewTxnArgs {
            graph: "delegated-graph".into(),
            tenant_scope: owner.tenant_scope().into(),
            begin_version: core.version(),
            isolation: crate::server::txn::IsolationLevel::Snapshot,
            predicate: None,
            agent: owner.owner_scope().into(),
            now_ms: 1,
        },
    );
    txn.write_set.push(Method::AddNode {
        node_id: "delegated-node".into(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type":"Record"})).unwrap(),
    });
    let parent = crate::server::handlers::admin::scope_admin_saga_authority(owner.clone(), async {
        let (receipt, _) = begin_txn_receipt(
            Some(backend.clone()),
            10,
            Some("same-principal"),
            "delegated-txn",
            &txn,
            Some("delegated-key"),
            None,
        )
        .unwrap();
        let parent = receipt_coordinator_id(&receipt);
        assert!(commit_cross_modal_txn_with_nonce(
            state,
            11,
            Some("same-principal"),
            &parent,
            txn,
            None
        )
        .await
        .unwrap());
        drop(receipt); // lost response: child committed, parent remains Prepared
        parent
    })
    .await;
    let child = crate::server::mutation_batch::opaque_coordinator_key(
        "crossmodal",
        "delegated-graph",
        &parent,
    );
    let record = backend
        .read_mutation_batch("delegated-graph", &child)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.committing_actor().unwrap(), owner.actor_scope());
    assert_ne!(
        record.committing_actor().unwrap(),
        crate::server::mutation_batch::principal_fingerprint("same-principal").unwrap()
    );
    let denied = crate::server::handlers::admin::scope_admin_saga_authority(foreign, async {
        reconcile_committed_txn(
            state,
            12,
            Some("same-principal"),
            "delegated-txn",
            Some("delegated-key"),
            Some(owner.tenant_scope()),
            None,
        )
        .await
    })
    .await
    .unwrap_err();
    assert!(denied.contains("principal"), "{denied}");
    let version = core.version();
    let replay = crate::server::handlers::admin::scope_admin_saga_authority(owner.clone(), async {
        reconcile_committed_txn(
            state,
            13,
            Some("same-principal"),
            "delegated-txn",
            Some("delegated-key"),
            Some(owner.tenant_scope()),
            None,
        )
        .await
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(replay.result, Some(ResultPayload::Bool(true))));
    assert_eq!(core.version(), version);
}
