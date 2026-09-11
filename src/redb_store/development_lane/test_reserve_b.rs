use super::*;

#[test]
fn authoritative_snapshot_and_row_delta_paths_refuse_orphaning_lane_work_item() {
    use crate::mutation_batch::{
        DurabilityDomain, IncarnationId, LogicalName, MutationOperation, MutationOutboxIntent,
        MutationScopeIdentity, MutationStateDescriptor, MutationSurface, ScopeTenantId,
        VersionExpectation, MUTATION_BATCH_VERSION,
    };
    use sha2::{Digest, Sha256};

    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:state-path"), TEST_NOW));
    let hold = accepted.hold.expect("state-path hold");
    let mut linked = crate::graph::GraphCore::new().snapshot();
    fixture.with_read(|read| {
        let nodes = read
            .scoped_owner_table(NODES)
            .expect("open linked WorkItem table");
        let bytes = nodes
            .get((TEST_GRAPH, hold.work_item_id.as_str()))
            .expect("lookup linked WorkItem")
            .expect("linked WorkItem exists")
            .value()
            .to_vec();
        linked
            .nodes
            .push((hold.work_item_id.clone(), std::sync::Arc::new(bytes)));
    });

    let make_batch =
        |batch_id: &str, key: &str, algorithm: &str, state: &[u8], source_version: u64| {
            let identity = MutationScopeIdentity::graph(
                ScopeTenantId::new("tenant:a").expect("static tenant id is valid"),
                LogicalName::new(TEST_GRAPH).expect("static graph name is valid"),
                IncarnationId::new("incarnation:test:development-lane")
                    .expect("static incarnation id is valid"),
            );
            let mut batch = crate::mutation_batch::MutationBatch {
                schema_version: MUTATION_BATCH_VERSION,
                batch_id: batch_id.into(),
                envelope: super::super::super::fixture_operation_envelope(
                    &identity,
                    &format!("principal:sha256:{}", "a".repeat(64)),
                    700,
                    key,
                ),
                identity,
                placement_epoch: 1,
                version_expectation: VersionExpectation::Graph(source_version),
                fencing_token: Some(1),
                authoritative_state: Some(MutationStateDescriptor {
                    algorithm: algorithm.into(),
                    digest: hex::encode(Sha256::digest(state)),
                    source_graph_version: source_version,
                    target_graph_version: source_version + 1,
                }),
                operations: vec![MutationOperation {
                    ordinal: 0,
                    surface: MutationSurface::Query,
                    domain: DurabilityDomain::GraphSnapshot,
                    method: Method::ApplyMutation {
                        event_type: "authoritative_state_operation".into(),
                        query: "sha256:state-path".into(),
                    },
                }],
                outbox: vec![MutationOutboxIntent {
                    topic: "state-path.test".into(),
                    key: batch_id.into(),
                    payload: Vec::new(),
                    headers: std::collections::BTreeMap::new(),
                }],
                created_at_ms: TEST_NOW,
            };
            batch
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("a fixture batch reseals its envelope over its final body");
            batch
        };

    let mut orphaned = linked.clone();
    orphaned.nodes.clear();
    let orphaned_state = orphaned.to_msgpack().expect("encode orphaned snapshot");
    let orphaned_batch = make_batch(
        "state-path-orphaned",
        "state-path-orphaned",
        "sha256",
        &orphaned_state,
        0,
    );
    #[cfg(feature = "security")]
    let mut orphaned_audit = super::super::super::AuditTailCache::new();
    assert!(super::super::super::commit_mutation_batch_state(
        &fixture.shard,
        super::super::super::StateCommitInput {
            graph_fname: TEST_GRAPH,
            batch: &orphaned_batch,
            authoritative_state_msgpack: &orphaned_state,
            result_msgpack: None,
            committed_at_ms: TEST_NOW,
            audited: true,
        },
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut orphaned_audit,
    )
    .is_err());

    // RMDD-29's native WorkItem-authority migration
    // (`work_item_capability::validate_snapshot_nodes`, invoked from
    // `commit_mutation_batch_state` for every `AuthoritativeGraphState::Snapshot`) now
    // unconditionally refuses any state-snapshot commit containing a WorkItem-shaped node
    // whose `status` is not `submitted`/`ready` -- by design, a snapshot commit always
    // purges native claim state, so it can never carry forward an ACTIVE lease. `linked`
    // carries this fixture's live (`status: "running"`) lane WorkItem, so this "exact
    // linked WorkItem" commit -- which predates that migration and originally expected
    // success -- is now ALSO refused, for a broader (not lane-specific) reason than
    // `orphaned` above. The invariant this test exists to prove -- a state-snapshot commit
    // can never orphan OR silently carry forward a live lane authority -- holds a fortiori:
    // NEITHER commit lands, and the query below proves the original hold is completely
    // untouched.
    let linked_state = linked.to_msgpack().expect("encode linked snapshot");
    let linked_batch = make_batch(
        "state-path-linked",
        "state-path-linked",
        "sha256",
        &linked_state,
        0,
    );
    #[cfg(feature = "security")]
    let mut linked_audit = super::super::super::AuditTailCache::new();
    assert!(super::super::super::commit_mutation_batch_state(
        &fixture.shard,
        super::super::super::StateCommitInput {
            graph_fname: TEST_GRAPH,
            batch: &linked_batch,
            authoritative_state_msgpack: &linked_state,
            result_msgpack: None,
            committed_at_ms: TEST_NOW,
            audited: true,
        },
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut linked_audit,
    )
    .is_err());

    // Neither `orphaned` nor `linked` above ever committed, so the durable graph version
    // is still 0 (unchanged from `commit_ops` never being called on this fixture's graph).
    let after = crate::graph::GraphCore::new().snapshot();
    let delta = crate::graph_delta::GraphRowDelta::between(&linked, &after)
        .expect("build orphaning row delta");
    let delta_state = delta.to_msgpack().expect("encode orphaning row delta");
    let delta_batch = make_batch(
        "state-path-delta",
        "state-path-delta",
        crate::graph_delta::ROW_DELTA_ALGORITHM,
        &delta_state,
        0,
    );
    #[cfg(feature = "security")]
    let mut delta_audit = super::super::super::AuditTailCache::new();
    assert!(super::super::super::commit_mutation_batch_state(
        &fixture.shard,
        super::super::super::StateCommitInput {
            graph_fname: TEST_GRAPH,
            batch: &delta_batch,
            authoritative_state_msgpack: &delta_state,
            result_msgpack: None,
            committed_at_ms: TEST_NOW,
            audited: true,
        },
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut delta_audit,
    )
    .is_err());

    let query = read_development_lane(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneQueryRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref,
            hold_id: hold.hold_id,
            now_ms: 0,
        },
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("query state-path hold after all three refused restores");
    assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
}

#[test]
fn every_low_level_graph_row_path_refuses_orphaning_a_lane_work_item() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:row-path"), TEST_NOW));
    let hold = accepted.hold.expect("row-path hold");
    let remove = Method::RemoveNode {
        node_id: hold.work_item_id.clone(),
    };

    let mut ops = vec![(TEST_GRAPH.to_string(), remove.clone())];
    let mut raft_log = Vec::new();
    #[cfg(feature = "security")]
    let mut audit_tail = super::super::super::AuditTailCache::new();
    assert!(super::super::super::commit_ops(
        &fixture.shard,
        &mut ops,
        &mut raft_log,
        "lane-row-path",
        TEST_NOW,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut audit_tail,
    )
    .is_err());

    #[cfg(feature = "security")]
    let mut crossmodal_audit = super::super::super::AuditTailCache::new();
    assert!(super::super::super::commit_crossmodal(
        &fixture.shard,
        TEST_GRAPH,
        super::super::super::CrossModalStaged {
            methods: std::slice::from_ref(&remove),
            ..Default::default()
        },
        "lane-row-path-crossmodal",
        TEST_NOW,
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut crossmodal_audit,
    )
    .is_err());

    let query = read_development_lane(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneQueryRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref,
            hold_id: hold.hold_id,
            now_ms: 0,
        },
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("query row-path hold after refused writes");
    assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
}
