//! R066 acceptance uses served decisions and the real persistent retention path.
use super::*;

async fn library_record(h: &Harness) -> StatisticalDecisionRecord {
    h.publish(
        "retained-tool",
        AgentComponentKind::Tool,
        "web search",
        None::<&()>,
        None,
    )
    .unwrap();
    let (schema, _) = publish_route_schema(h);
    decide_and_assert_abstains(h, &schema, None)
        .await
        .records
        .as_slice()[0]
        .clone()
}

#[cfg(feature = "blob")]
async fn commit_record(h: &Harness, record: StatisticalDecisionRecord) -> DecisionLogEntry {
    let id = record.record_id.clone();
    let _: DecisionLogCommitted = decode(
        log_op(
            h,
            "decider",
            DecisionLogOp::Commit {
                record: Box::new(record),
                evaluator: None,
            },
        )
        .await,
    )
    .unwrap();
    log_tests::log_entry(h, &id)
}

#[cfg(feature = "blob")]
async fn compact_at(
    h: &Harness,
    policy: &ComponentDependency,
    now_ms: u64,
) -> eg_types::decision::statistical::log::DecisionLogCompacted {
    let state = h.state.read().await;
    let retention = super::super::stat_retention::Retention::of(&state, &verified());
    let ctx = super::super::stat_executor::ExecutionContext {
        store: &h.store,
        tenant_id: TENANT,
        now_ms,
        server_secret: state.auth_secret.as_bytes(),
    };
    super::super::stat_retention::compact(
        &ctx,
        &retention,
        &DecisionPolicyRef::Pinned {
            component: policy.clone(),
        },
        16,
    )
    .unwrap()
}

// spec: EG-DECISION-ENGINE-R066.1
#[cfg(feature = "blob")]
#[tokio::test]
async fn library_records_survive_every_retention_window() {
    let h = Harness::new().await;
    let record = library_record(&h).await;
    let entry = commit_record(&h, record).await;
    let before = serde_json::to_vec(&entry).unwrap();
    let policy = h.publish_policy("retain-library", &log_tests::retention_policy(0, Some(0)));
    for now in [entry.committed_at_ms, u64::MAX, u64::MAX] {
        let changed = compact_at(&h, &policy, now).await;
        assert_eq!((changed.compacted, changed.retired), (0, 0));
        assert_eq!(
            serde_json::to_vec(&log_tests::log_entry(&h, &entry.record.record_id)).unwrap(),
            before
        );
    }
    assert!(matches!(
        log_tests::verify_log(&h, &entry.record.record_id)
            .await
            .unwrap(),
        eg_types::decision::statistical::log::DecisionLogVerification::Verified { .. }
    ));
}

// spec: EG-DECISION-ENGINE-R066.2
#[cfg(all(feature = "blob", feature = "query"))]
#[tokio::test]
async fn graph_inputs_expire_at_the_configured_boundaries() {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    let record = graph_log_record(&h).await;
    let entry = commit_record(&h, record).await;
    let policy = h.publish_policy(
        "expire-graph-inputs",
        &log_tests::retention_policy(100, Some(200)),
    );
    let start = entry.committed_at_ms;
    for (now, expected) in [
        (start + 99, (0, 0)),
        (start + 100, (1, 0)),
        (start + 299, (0, 0)),
        (start + 300, (0, 1)),
        (u64::MAX, (0, 0)),
    ] {
        let changed = compact_at(&h, &policy, now).await;
        assert_eq!((changed.compacted, changed.retired), expected);
        assert_eq!(
            log_tests::log_entry(&h, &entry.record.record_id)
                .record
                .record_digest,
            entry.record.record_digest
        );
    }
    assert!(matches!(
        log_tests::verify_log(&h, &entry.record.record_id)
            .await
            .unwrap(),
        eg_types::decision::statistical::log::DecisionLogVerification::InputsRetired { .. }
    ));
}

// spec: EG-DECISION-ENGINE-R066.3
#[tokio::test]
async fn evaluate_only_leaves_no_committed_records() {
    let h = Harness::new().await;
    let record = library_record(&h).await;
    let schema = record.inputs.feature_schema;
    for _ in 0..16 {
        let batch = decide_and_assert_abstains(&h, &schema, None).await;
        for record in batch.records.as_slice() {
            assert!(h
                .store
                .decision_record_body(TENANT, &record.record_id)
                .unwrap()
                .is_none());
            assert!(h
                .store
                .current_component(TENANT, &record.record_id)
                .unwrap()
                .is_none());
        }
    }
    assert!(h
        .store
        .decision_artifacts_with_prefix(TENANT, "record:", 100)
        .unwrap()
        .is_empty());
}
