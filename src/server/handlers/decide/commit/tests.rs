//! The served guard must refuse before either durable owner changes.

use std::sync::Arc;

use eg_compute::assemble::{assemble, inputs, replay_check, RecordIdentity};
use eg_types::capacity_lease::{CapacityCell, CapacityResourceClass};
use eg_types::contract::BoundedVec;
use eg_types::decision::{
    CapacityHeadroom, DecisionCommitRequest, DecisionCommitResult, DecisionRecord, SchemaAuthority,
    TemplateFacts, TopologyAdmission, TopologyInputs,
};
use eg_types::delegation::AgentGraphEntryRef;
use eg_types::native_control::{
    CapacityCellUpdateRequest, CapacityStatusRequest, NativeControlSchemaVersion,
};
use eg_types::test_support::topology::{facts, requirements, FAN_OUT_JOIN, INDEPENDENT_SUBTASKS};
use tokio::sync::RwLock;

use crate::protocol::{Method, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_record::tests::{
    commit_context, decided, seed_library, TENANT,
};
use crate::server::persistence::redb_backend::RedbBackend;
use crate::server::persistence::PersistenceBackend;
use crate::server::state::ServerState;

fn topology_record(store: &AgentLibraryStore) -> DecisionRecord {
    let base = decided(store);
    let mut topology = facts();
    let shape = base.graph.unwrap().shape;
    let mut slot = topology.slots.as_slice()[0].clone();
    slot.node_id = shape.entry_node.clone();
    topology.slots = BoundedVec::new(vec![slot]).unwrap();
    let template = TemplateFacts {
        graph_id: "topology-template".into(),
        entry_revision: 1,
        definition_digest: "a".repeat(64),
        shape,
        topology: Some(topology),
    };
    let mut request = base.record.inputs.request;
    request.requirements.topology = Some(requirements(1));
    request.templates = BoundedVec::new(vec![AgentGraphEntryRef {
        tenant_id: TENANT.into(),
        graph_id: template.graph_id.clone(),
        entry_revision: template.entry_revision,
        definition_digest: template.definition_digest.clone(),
        actor_scope: "operator".into(),
        purpose_id: "agent-graph:publish".into(),
        policy_digest: "b".repeat(64),
        composed_work_ceiling: 8,
    }])
    .unwrap();
    let mut input = inputs(
        request,
        base.record.inputs.candidates.as_slice().to_vec(),
        vec![template],
        base.record.inputs.policy,
    )
    .unwrap();
    input.topology = Some(TopologyInputs {
        schema_digest: format!("sha256:{}", "ab".repeat(32)),
        admissions: BoundedVec::new(vec![TopologyAdmission {
            task_class: INDEPENDENT_SUBTASKS.into(),
            topology_class: FAN_OUT_JOIN.into(),
            admit_class: format!("{FAN_OUT_JOIN}Admission"),
            source_key: "swarm-topology".into(),
            authority: SchemaAuthority::Admin,
            axioms: BoundedVec::default(),
        }])
        .unwrap(),
        verify_required_by: BoundedVec::default(),
        headroom: BoundedVec::new(vec![CapacityHeadroom {
            cell_id: "cell-llm".into(),
            class: CapacityResourceClass::LlmGenerator,
            available: 8,
            epoch: 1,
        }])
        .unwrap(),
    });
    let record = assemble(
        input,
        RecordIdentity {
            tenant_id: TENANT.into(),
            caller_principal: "caller-a".into(),
            created_at_ms: 20,
        },
    )
    .unwrap()
    .record;
    replay_check(&record).expect("genuine topology record replays");
    assert!(record
        .topology_plan()
        .unwrap()
        .lease
        .per_cell
        .iter()
        .any(|cell| cell.amount > 0));
    record
}

fn owner_bytes(dir: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "redb")
        })
        .map(|path| {
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

// spec: EG-DECISION-ENGINE-R113.2.1
#[tokio::test]
async fn topology_commit_refuses_uncoordinated_owners() {
    #[cfg(any(feature = "security", feature = "raft"))]
    let _env_guard = crate::crypto::acquire_test_env_read_lock().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let backend = Arc::new(RedbBackend::open_with_shards(path.clone(), 16, 1).unwrap());
    backend
        .commit_capacity_lease(
            "topology",
            Method::UpdateCapacityCell {
                request: CapacityCellUpdateRequest {
                    schema_version: NativeControlSchemaVersion::V1,
                    cell: CapacityCell {
                        cell_id: "cell-llm".into(),
                        parent_id: None,
                        resource_class: CapacityResourceClass::LlmGenerator,
                        capacity: 8,
                        reserved_floor: 0,
                        epoch: 1,
                        policy_digest: "d".repeat(64),
                        updated_at_ms: 0,
                        throttle: None,
                    },
                    expected_epoch: None,
                    now_ms: 1,
                },
            },
        )
        .await
        .unwrap();
    let mut server =
        ServerState::new_for_test("commit-guard", crate::isolation::IsolationLayer::new());
    server.persist_dir = Some(path);
    server.persistence = Some(backend.clone());
    let state = Arc::new(RwLock::new(server));
    let store = state.write().await.ensure_agent_library().unwrap();
    seed_library(&store);
    let record = topology_record(&store);
    let request = DecisionCommitRequest {
        context: commit_context(&store, "topology-commit", 1),
        expected_catalog_digest: record.inputs.catalog_digest.clone(),
        record: record.clone(),
    };
    let status = CapacityStatusRequest {
        schema_version: NativeControlSchemaVersion::V1,
        tenant_ref: TENANT.into(),
        cell_id: Some("cell-llm".into()),
        lease_id: None,
        max_count: 10,
        cursor: None,
    };
    let capacity_before = backend
        .read_capacity_status("topology", &status)
        .await
        .unwrap();
    let before = owner_bytes(dir.path());
    assert!(before.len() >= 2, "both durable owner files exist");
    let claims = VerifiedRequestContext::verified_for_test_in_tenant("caller-a", TENANT);
    let verified = VerifiedRequestContext::from_verified_claims_with_nonce(
        claims.claims().clone(),
        "ordinary-commit".into(),
        Some(eg_types::contract::Nonce::from_bytes([3; 32])),
    );
    let response = super::handle_decision_commit(&state, 1, &verified, request.clone()).await;
    assert_eq!(response.error.as_deref(), Some("DERIVATION_REJECTED"));
    assert_eq!(
        response.error_detail.as_deref(),
        Some("topology capacity acquisition requires shared transaction authority")
    );
    assert!(response.result.is_none());
    assert_eq!(
        owner_bytes(dir.path()),
        before,
        "neither owner writes, including receipts and outbox"
    );
    assert_eq!(
        backend
            .read_capacity_status("topology", &status)
            .await
            .unwrap(),
        capacity_before
    );
    assert!(store
        .current_component(TENANT, &record.record_id)
        .unwrap()
        .is_none());
    let foreign = VerifiedRequestContext::verified_for_test_in_tenant("caller-a", "foreign");
    let response = super::handle_decision_commit(&state, 2, &foreign, request).await;
    assert_eq!(response.error.as_deref(), Some("ACCESS_DENIED"));
    assert_eq!(owner_bytes(dir.path()), before);

    // The same served path still accepts an ordinary, replay-valid record.
    let plain = decided(&store).record;
    let response = super::handle_decision_commit(
        &state,
        3,
        &verified,
        DecisionCommitRequest {
            context: commit_context(&store, "ordinary-commit", 3),
            expected_catalog_digest: plain.inputs.catalog_digest.clone(),
            record: plain.clone(),
        },
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("commit result")
    };
    let committed: DecisionCommitResult = rmp_serde::from_slice(&bytes).unwrap();
    assert_eq!(committed.record_id, plain.record_id);
    assert!(store
        .current_component(TENANT, &plain.record_id)
        .unwrap()
        .is_some());
    assert_eq!(
        backend
            .read_capacity_status("topology", &status)
            .await
            .unwrap(),
        capacity_before
    );
}
