use super::*;

#[test]
fn native_cancel_advances_fence_and_old_finish_replays_after_reopen() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:cancel"), TEST_NOW));
    let hold = accepted.hold.expect("accepted cancel hold");
    let finish_request = DevelopmentLaneFinishRequest {
        schema_version: DevelopmentLaneFinishRequestSchemaVersion::V1,
        tenant_ref: hold.tenant_ref.clone(),
        work_item_id: hold.work_item_id.clone(),
        owner_id: hold.owner_id.clone(),
        attempt: hold.attempt,
        lease_epoch: hold.lease_epoch,
        fencing_token: hold.fencing_token,
        work_item_fence: hold.work_item_fence.clone(),
        hold_id: hold.hold_id.clone(),
        expected_hold_revision: hold.hold_revision,
        terminal_state: DevelopmentLaneFinishRequestTerminalState::Cancelled,
        idempotency_key: "finish:cancel-old-tuple".into(),
        now_ms: 0,
    };

    let committed = fixture
        .cancel_work_item("cancel-terminal", 2_000_000)
        .expect("expired WorkItem cancellation");
    assert!(!committed.replayed);
    let status = read_development_lane_status(
        &fixture.shard,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref.clone(),
            hold_id: Some(hold.hold_id.clone()),
            lane_id: None,
            work_item_id: Some(hold.work_item_id.clone()),
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        2_000_000,
        DurableCrypto::none(),
    )
    .expect("read atomically cancelled lane");
    let cancelled_hold = status.holds[0].clone();
    assert_eq!(
        cancelled_hold.state,
        DevelopmentLaneHoldState::CleanupPending
    );
    assert!(!cancelled_hold.active_count_charged);
    assert_eq!(cancelled_hold.retained_disk_bytes, 10);
    assert_eq!(cancelled_hold.lease_epoch, hold.lease_epoch + 1);
    assert_eq!(cancelled_hold.fencing_token, hold.fencing_token + 1);
    assert_eq!(status.tenant_active_count, 0);
    assert_eq!(status.tenant_retained_disk_bytes, 10);

    let path = fixture.close_for_reopen();
    let shard = Shard::open(&path).expect("reopen cancelled lane shard");
    let finished: DevelopmentLaneFinishResult = decode_durable(
        &commit_development_lane(
            &shard,
            TEST_GRAPH,
            &Method::FinishDevelopmentLane {
                request: finish_request.clone(),
            },
            2_000_000,
            DurableCrypto::none(),
        )
        .expect("old tuple finish must repair cancelled lane"),
    )
    .expect("decode replayed finish result");
    assert_eq!(
        finished.decision,
        DevelopmentLaneFinishResultDecision::Idempotent
    );
    assert_eq!(finished.hold, Some(cancelled_hold.clone()));

    // A fresh caller may use the retained, post-cancel WorkItem tuple; it
    // must observe the same terminal authority without changing counters.
    let mut current_tuple = finish_request.clone();
    current_tuple.idempotency_key = "finish:cancel-current-tuple".into();
    current_tuple.attempt = cancelled_hold.attempt;
    current_tuple.lease_epoch = cancelled_hold.lease_epoch;
    current_tuple.fencing_token = cancelled_hold.fencing_token;
    current_tuple.work_item_fence = cancelled_hold.work_item_fence.clone();
    current_tuple.expected_hold_revision = cancelled_hold.hold_revision;
    let current: DevelopmentLaneFinishResult = decode_durable(
        &commit_development_lane(
            &shard,
            TEST_GRAPH,
            &Method::FinishDevelopmentLane {
                request: current_tuple,
            },
            2_000_000,
            DurableCrypto::none(),
        )
        .expect("current tuple finish replay"),
    )
    .expect("decode current tuple finish result");
    assert_eq!(
        current.decision,
        DevelopmentLaneFinishResultDecision::Idempotent
    );

    let mut wrong_fence = finish_request;
    wrong_fence.idempotency_key = "finish:cancel-wrong-fence".into();
    wrong_fence.lease_epoch = cancelled_hold.lease_epoch;
    wrong_fence.fencing_token = cancelled_hold.fencing_token;
    wrong_fence.work_item_fence = "fence:wrong".into();
    let refused: DevelopmentLaneFinishResult = decode_durable(
        &commit_development_lane(
            &shard,
            TEST_GRAPH,
            &Method::FinishDevelopmentLane {
                request: wrong_fence,
            },
            2_000_000,
            DurableCrypto::none(),
        )
        .expect("wrong fence finish response"),
    )
    .expect("decode wrong fence finish result");
    assert_eq!(
        refused.decision,
        DevelopmentLaneFinishResultDecision::WrongFence
    );
    drop(shard);
    std::fs::remove_file(path).expect("remove reopened cancellation database");
}
