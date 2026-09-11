use super::*;

#[test]
fn native_reserve_links_live_resource_and_replays_or_refuses_input_atomically() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:one"), TEST_NOW));
    assert_eq!(accepted.decision, DevelopmentLaneResultDecision::Accepted);
    let hold = accepted.hold.clone().expect("accepted hold");
    assert_eq!(hold.state, DevelopmentLaneHoldState::Active);
    assert_eq!(hold.host_ref, REDACTED_PRIVATE_ID);
    assert_eq!(hold.worktree_locator, REDACTED_PRIVATE_ID);
    assert!(hold.host_target_alias.is_none());

    let replay: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:one"), TEST_NOW));
    assert_eq!(replay, accepted);

    let mut conflict_request = fixture.reserve.clone();
    conflict_request.idempotency_key = "reserve:one".into();
    conflict_request.intent.branch = "branch:conflict".into();
    let conflict: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: conflict_request,
        },
        TEST_NOW,
    ));
    assert_eq!(
        conflict.decision,
        DevelopmentLaneResultDecision::InputConflict
    );
    let query = read_development_lane(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneQueryRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneQueryRequestSchemaVersion::V1,
            tenant_ref: "tenant:a".into(),
            hold_id: hold.hold_id,
            now_ms: 0,
        },
        TEST_NOW,
        DurableCrypto::none(),
    )
    .expect("query accepted hold");
    assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
}

#[test]
fn native_work_item_authority_rejects_generic_kind_and_fence_aliases() {
    let fixture = NativeLaneFixture::new(policy());
    let work_item_id = fixture.reserve.work_item_id.clone();
    fixture.mutate_work_item(&work_item_id, |props| {
        props.remove("kind");
        props.insert("work_item_kind".into(), serde_json::json!("lane.lifecycle"));
    });
    let mut kind_request = fixture.reserve.clone();
    kind_request.idempotency_key = "reserve:generic-kind".into();
    let kind_refusal: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: kind_request,
        },
        TEST_NOW,
    ));
    assert_eq!(
        kind_refusal.decision,
        DevelopmentLaneResultDecision::WrongKind
    );

    let fixture = NativeLaneFixture::new(policy());
    let work_item_id = fixture.reserve.work_item_id.clone();
    fixture.mutate_work_item(&work_item_id, |props| {
        props.remove("work_item_fence");
        props.insert("fence".into(), serde_json::json!("fence:request:initial"));
    });
    let mut fence_request = fixture.reserve.clone();
    fence_request.idempotency_key = "reserve:generic-fence".into();
    let fence_refusal: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: fence_request,
        },
        TEST_NOW,
    ));
    assert_eq!(
        fence_refusal.decision,
        DevelopmentLaneResultDecision::WrongFence
    );

    let fixture = NativeLaneFixture::new(policy());
    let work_item_id = fixture.reserve.work_item_id.clone();
    let forged_intent =
        serde_json::to_value(&fixture.reserve.intent).expect("encode forged generic lane intent");
    fixture.mutate_work_item(&work_item_id, |props| {
        if let Some(metadata) = props
            .get_mut("metadata")
            .and_then(|value| value.as_object_mut())
        {
            metadata.remove("repository_work_item");
        }
        props.insert("development_lane_intent".into(), forged_intent);
    });
    let mut intent_request = fixture.reserve.clone();
    intent_request.idempotency_key = "reserve:generic-intent".into();
    let intent_refusal: DevelopmentLaneResult = fixture.decode(&fixture.commit(
        Method::ReserveDevelopmentLane {
            request: intent_request,
        },
        TEST_NOW,
    ));
    assert_eq!(
        intent_refusal.decision,
        DevelopmentLaneResultDecision::InputConflict
    );
}

#[test]
fn graph_lifecycle_clear_fails_closed_while_lane_hold_is_live() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:clear-live"), TEST_NOW));
    assert_eq!(accepted.decision, DevelopmentLaneResultDecision::Accepted);
    let refusal = clear_native_graph_rows(&fixture.shard, TEST_GRAPH, DurableCrypto::none());
    assert!(
        refusal.is_err(),
        "live lane authority must block graph clear"
    );

    let hold = accepted
        .hold
        .expect("live hold remains after refused clear");
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
    .expect("query live hold after refused clear");
    assert_eq!(query.decision, DevelopmentLaneQueryResultDecision::Accepted);
}
