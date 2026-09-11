use super::*;

#[test]
fn native_cancel_lost_ack_is_replay_safe_across_crash_and_reopen() {
    let fixture = NativeLaneFixture::new(policy());
    let accepted: DevelopmentLaneResult =
        fixture.decode(&fixture.commit(fixture.reserve_method("reserve:cancel-crash"), TEST_NOW));
    let hold = accepted.hold.expect("accepted crash hold");
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
        idempotency_key: "finish:cancel-crash-repair".into(),
        now_ms: 0,
    };
    let crash = fixture.cancel_work_item_with_crash(
        "cancel-crash",
        2_000_000,
        Some(super::super::super::MutationBatchCrashpoint::AfterCommitBeforeAck),
    );
    assert_eq!(crash.unwrap_err(), "injected crash");
    let path = fixture.close_for_reopen();
    let shard = Shard::open(&path).expect("reopen crash shard");
    let status = read_development_lane_status(
        &shard,
        TEST_GRAPH,
        &DevelopmentLaneStatusRequest {
            schema_version:
                crate::epistemic_operations::DevelopmentLaneStatusRequestSchemaVersion::V1,
            tenant_ref: hold.tenant_ref,
            hold_id: Some(hold.hold_id),
            lane_id: None,
            work_item_id: None,
            limit: 10,
            cursor: None,
            now_ms: 0,
        },
        2_000_000,
        DurableCrypto::none(),
    )
    .expect("read committed cancellation after lost ack");
    assert_eq!(status.tenant_active_count, 0);
    assert_eq!(status.tenant_retained_disk_bytes, 10);
    let finished: DevelopmentLaneFinishResult = decode_durable(
        &commit_development_lane(
            &shard,
            TEST_GRAPH,
            &Method::FinishDevelopmentLane {
                request: finish_request,
            },
            2_000_000,
            DurableCrypto::none(),
        )
        .expect("repair lost cancellation acknowledgement"),
    )
    .expect("decode crash repair result");
    assert_eq!(
        finished.decision,
        DevelopmentLaneFinishResultDecision::Idempotent
    );
    drop(shard);
    std::fs::remove_file(path).expect("remove reopened crash database");
}
