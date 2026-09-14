//! Transaction recovery and stable replay-intent checks.

use super::*;

#[test]
fn recovery_plan_is_canonical_and_omits_raw_agent_identity() {
    let core = GraphCore::new();
    let mut first = GraphTxnState::new(
        &core,
        NewTxnArgs {
            graph: "logical-graph".to_string(),
            tenant_scope: "opaque-tenant-scope".to_string(),
            begin_version: 7,
            isolation: IsolationLevel::Snapshot,
            predicate: None,
            agent: "raw-personal-identity".to_string(),
            now_ms: 10,
        },
    );
    first.write_set.push(Method::RemoveNode {
        node_id: "node-a".to_string(),
    });
    first
        .read_set
        .insert("node-b".to_string(), NodeFingerprint::Absent);
    first
        .read_set
        .insert("node-a".to_string(), NodeFingerprint::Present(42));

    let mut second = first.clone();
    second.read_set.clear();
    second
        .read_set
        .insert("node-a".to_string(), NodeFingerprint::Present(42));
    second
        .read_set
        .insert("node-b".to_string(), NodeFingerprint::Absent);

    let encoded = first.encode_recovery_plan().unwrap();
    assert_eq!(encoded, second.encode_recovery_plan().unwrap());
    assert!(!encoded
        .windows(b"raw-personal-identity".len())
        .any(|window| window == b"raw-personal-identity"));

    let recovered = GraphTxnState::decode_recovery_plan(
        &encoded,
        "retry-identity-held-only-in-ram".to_string(),
    )
    .unwrap();
    assert_eq!(recovered.graph, "logical-graph");
    assert_eq!(recovered.tenant_scope, "opaque-tenant-scope");
    assert_eq!(recovered.begin_version, 7);
    assert_eq!(recovered.agent, "retry-identity-held-only-in-ram");
    assert_eq!(recovered.read_set, first.read_set);
}

fn staged_intent() -> GraphTxnState {
    GraphTxnState::new(
        &GraphCore::new(),
        NewTxnArgs {
            graph: "graph-a".into(),
            tenant_scope: "tenant-a".into(),
            begin_version: 1,
            isolation: IsolationLevel::Serializable,
            predicate: Some(PredicateRead::Label("Record".into())),
            agent: "owner".into(),
            now_ms: 1,
        },
    )
}

#[test]
fn replay_intent_ignores_observations_but_recovery_preserves_them() {
    let mut first = staged_intent();
    first
        .read_set
        .insert("node".into(), NodeFingerprint::Absent);
    let mut retry = first.clone();
    retry.begin_version = 999;
    retry.last_active_ms = 999;
    retry.agent = "reconstructed-owner".into();
    retry
        .read_set
        .insert("node".into(), NodeFingerprint::Present(999));
    retry.predicate_reads[0].1 = 999;
    assert_eq!(
        first.replay_intent_digest().unwrap(),
        retry.replay_intent_digest().unwrap()
    );
    assert_ne!(
        first.encode_recovery_plan().unwrap(),
        retry.encode_recovery_plan().unwrap()
    );
    let recovered =
        GraphTxnState::decode_recovery_plan(&retry.encode_recovery_plan().unwrap(), "owner".into())
            .unwrap();
    assert_eq!(recovered.read_set, retry.read_set);
    assert_eq!(recovered.begin_version, 999);
    assert_eq!(recovered.predicate_reads[0].1, 999);
}

#[test]
fn replay_intent_binds_every_durable_write_surface_and_read_definition() {
    let original = staged_intent();
    let mutations: &[fn(&mut GraphTxnState)] = &[
        |txn| txn.graph.push('b'),
        |txn| txn.tenant_scope.push('b'),
        |txn| {
            txn.write_set.push(Method::RemoveNode {
                node_id: "n".into(),
            })
        },
        |txn| {
            txn.read_set
                .insert("read-key".into(), NodeFingerprint::Absent);
        },
        |txn| txn.isolation = IsolationLevel::Snapshot,
        |txn| txn.predicate_reads[0].0 = PredicateRead::Label("Other".into()),
        |txn| {
            txn.extra_writes.insert(
                "other-graph".into(),
                vec![Method::RemoveNode {
                    node_id: "n".into(),
                }],
            );
        },
        |txn| txn.vectors.push(("n".into(), vec![0.5])),
        |txn| txn.blob_refs.push(("n".into(), "digest".into())),
        |txn| {
            txn.measurements.push(StagedMeasurement {
                series: "s".into(),
                n_fields: 1,
                bucket_ns: 1,
                field_names: vec!["v".into()],
                points: vec![(1, vec![0.5])],
            })
        },
        |txn| {
            txn.axioms.push(Method::RemoveNode {
                node_id: "axiom".into(),
            })
        },
        |txn| {
            txn.constructs.push(Method::RemoveNode {
                node_id: "construct".into(),
            })
        },
        |txn| {
            txn.plan_writeback.push(Method::RemoveNode {
                node_id: "plan".into(),
            })
        },
    ];
    let digest = original.replay_intent_digest().unwrap();
    for mutate in mutations {
        let mut changed = original.clone();
        mutate(&mut changed);
        assert_ne!(digest, changed.replay_intent_digest().unwrap());
    }
}
