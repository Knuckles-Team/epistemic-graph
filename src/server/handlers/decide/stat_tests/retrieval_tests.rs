//! Retrieval learning over the log (EH-394, EH-395, EH-396, EH-398), served:
//! outcomes join only their committer's executed retrieval-plan records,
//! nothing is learned before an independent verdict, and a query adapter is
//! fitted from judged runs, served only after its passing receipt, and rolled
//! back to the identity.

use super::consumer_tests::declared_abstention;
use super::*;
use eg_types::decision::statistical::retrieval::{
    HardNegativeRequest, HardNegativeSet, PathEdge, PathRank, PathRequest, ProvenPaths,
    QueryVector, RetrievalOp, RetrievalOutcome, RetrievalPathTemplate, RetrievalResult,
    ReturnedEvidence,
};
use eg_types::decision::statistical::retrieval_adapter::{
    AdapterFitRequest, AdapterFitted, AdapterState,
};

const GRAPH: &str = "kg-retrieval";

fn op(retrieval: RetrievalOp) -> DecisionLogOp {
    DecisionLogOp::Retrieval {
        tenant_id: TENANT.to_string(),
        retrieval,
    }
}

async fn learn(h: &Harness, who: &str, retrieval: RetrievalOp) -> Result<RetrievalResult, String> {
    decode(log_op(h, who, op(retrieval)).await)
}

/// A committed retrieval-plan record that EXECUTED `plan-hyde`, cloned under
/// `record_id`: the logged declared abstention with its outcome set to an
/// exploration (the log join reads the stored entry; it does not replay).
async fn executed_template(h: &Harness) -> DecisionLogEntry {
    let record = declared_abstention(h).await;
    let commit = DecisionLogOp::Commit {
        record: Box::new(record),
    };
    let logged: DecisionLogCommitted = decode(log_op(h, "decider", commit).await).unwrap();
    let key = crate::server::persistence::decision_jobs::record_key(&logged.record_id);
    let bytes = h.store.decision_artifact(TENANT, &key).unwrap().unwrap();
    crate::server::persistence::decision_jobs::decode_artifact(&bytes, "entry").unwrap()
}

fn clone_executed(h: &Harness, template: &DecisionLogEntry, record_id: &str) {
    let mut entry = template.clone();
    entry.record.record_id = record_id.to_string();
    entry.record.outcome = StatisticalOutcome::Explored {
        option_id: "plan-hyde".to_string(),
        propensity: rational(1, 1),
    };
    let key = crate::server::persistence::decision_jobs::record_key(record_id);
    let bytes = crate::server::persistence::decision_jobs::encode_artifact(&entry).unwrap();
    h.store
        .put_decision_artifacts(TENANT, &[(key, bytes)])
        .unwrap();
}

fn returned(ids: &[&str], class: &str) -> BoundedVec<ReturnedEvidence, 256> {
    BoundedVec::new(
        ids.iter()
            .map(|id| ReturnedEvidence {
                evidence_id: id.to_string(),
                content_class: Some(class.to_string()),
            })
            .collect(),
    )
    .unwrap()
}

fn outcome(record_id: &str, ids: &[&str], cited: &[&str]) -> RetrievalOutcome {
    RetrievalOutcome {
        record_id: record_id.to_string(),
        returned: returned(ids, "doc"),
        cited: BoundedVec::new(cited.iter().map(|c| c.to_string()).collect()).unwrap(),
        query: None,
        path: None,
    }
}

async fn evaluate_by(h: &Harness, who: &str, record_id: &str, success: bool) {
    let evaluation = DecisionLogOp::Evaluate {
        tenant_id: TENANT.to_string(),
        evaluation: DecisionOutcomeEvaluation {
            record_id: record_id.to_string(),
            evaluation_id: format!("eval-{who}"),
            class: EvidenceClass::Observation,
            selected_agent: "retriever-agent".to_string(),
            lease_holder: "retriever-worker".to_string(),
            fidelity: OutcomeFidelity::ToolCalls,
            success: Some(success),
        },
    };
    let _: StoredEvaluation = decode(log_op(h, who, evaluation).await).unwrap();
}

async fn negatives(h: &Harness, who: &str) -> HardNegativeSet {
    let request = HardNegativeRequest {
        question_id: None,
        window: window(),
        limit: 100,
    };
    match learn(h, who, RetrievalOp::HardNegatives { request })
        .await
        .unwrap()
    {
        RetrievalResult::HardNegatives(set) => set,
        other => panic!("hard negatives: {other:?}"),
    }
}

fn template(composed_digest: &str) -> RetrievalPathTemplate {
    RetrievalPathTemplate {
        task_class: "urn:task:incident-triage".to_string(),
        composed_digest: composed_digest.to_string(),
        policy_version: "1".to_string(),
        anchor_class: "Incident".to_string(),
        edges: BoundedVec::new(vec![PathEdge {
            relationship: "AFFECTS".to_string(),
            min_hops: 1,
            max_hops: 2,
        }])
        .unwrap(),
        rank: PathRank::Vector,
        slots: BoundedVec::default(),
        skill_ref: Some("skill.incident-triage".to_string()),
    }
}

async fn paths(h: &Harness, composed_digest: &str) -> ProvenPaths {
    let request = PathRequest {
        task_class: "urn:task:incident-triage".to_string(),
        composed_digest: composed_digest.to_string(),
        policy_version: None,
        window: window(),
    };
    match learn(h, "decider", RetrievalOp::Paths { request })
        .await
        .unwrap()
    {
        RetrievalResult::Paths(paths) => paths,
        other => panic!("paths: {other:?}"),
    }
}

#[tokio::test]
async fn outcomes_teach_nothing_until_an_independent_verdict_joins_them() {
    let h = Harness::new().await;
    let template_entry = executed_template(&h).await;
    clone_executed(&h, &template_entry, "rec-1");
    let units: Vec<String> = (0..12).map(|i| format!("e{i}")).collect();
    let ids: Vec<&str> = units.iter().map(String::as_str).collect();
    let mut attested = outcome("rec-1", &ids, &["e5"]);
    attested.path = Some(template("sha256:schema-a"));
    let record = |o: &RetrievalOutcome| RetrievalOp::RecordOutcome {
        outcome: Box::new(o.clone()),
    };

    let foreign = learn(&h, "stranger", record(&attested)).await;
    assert!(
        foreign.unwrap_err().starts_with("PARAMETER_INVALID"),
        "a stranger cannot see it"
    );
    let mut bogus = attested.clone();
    bogus.cited = BoundedVec::new(vec!["zz".to_string()]).unwrap();
    assert!(learn(&h, "decider", record(&bogus)).await.is_err());
    assert!(matches!(
        learn(&h, "decider", record(&attested)).await.unwrap(),
        RetrievalResult::Recorded(_)
    ));
    let mut conflicting = attested.clone();
    conflicting.cited = BoundedVec::new(vec!["e6".to_string()]).unwrap();
    let conflict = learn(&h, "decider", record(&conflicting)).await;
    assert!(conflict.unwrap_err().starts_with("IDEMPOTENCY_CONFLICT"));

    let unjudged = negatives(&h, "decider").await;
    assert_eq!(
        (unjudged.judged, unjudged.unjudged, unjudged.rows.len()),
        (0, 1, 0)
    );
    assert!(paths(&h, "sha256:schema-a").await.rows.is_empty());

    evaluate_by(&h, "decider", "rec-1", true).await;
    let self_judged = negatives(&h, "decider").await;
    assert_eq!(
        self_judged.judged, 0,
        "the attester's own verdict is never a label"
    );

    evaluate_by(&h, "evaluator", "rec-1", true).await;
    let judged = negatives(&h, "decider").await;
    assert_eq!(judged.judged, 1);
    let rows: Vec<(&str, u32)> = judged
        .rows
        .iter()
        .map(|n| (n.evidence_id.as_str(), n.rank))
        .collect();
    assert_eq!(
        rows,
        [("e0", 1), ("e1", 2), ("e2", 3), ("e3", 4), ("e4", 5)]
    );
    assert!(
        negatives(&h, "stranger").await.rows.is_empty(),
        "invisible record, no negatives"
    );

    paths_and_usage_follow_the_verdict(&h).await;
}

/// After the independent success: the path is proven under its schema
/// identity only, and the per-class usage counts the returned and cited units.
async fn paths_and_usage_follow_the_verdict(h: &Harness) {
    let proven = paths(h, "sha256:schema-a").await;
    assert_eq!(proven.rows.len(), 1);
    assert_eq!(
        (
            proven.rows.as_slice()[0].successes,
            proven.rows.as_slice()[0].failures
        ),
        (1, 0)
    );
    assert!(
        paths(h, "sha256:schema-b").await.rows.is_empty(),
        "a new schema identity retires it"
    );

    let usage = match learn(h, "decider", RetrievalOp::Usage { window: window() })
        .await
        .unwrap()
    {
        RetrievalResult::Usage(usage) => usage,
        other => panic!("usage: {other:?}"),
    };
    let doc = &usage.rows.as_slice()[0];
    assert_eq!(
        (doc.content_class.as_str(), doc.returned, doc.cited),
        ("doc", 12, 1)
    );
    assert_eq!(usage.outcomes, 1);
}

/// Nodes `p<i>` (what answers cite) are the unit axis 1 and `n<i>` (what the
/// base query prefers) the unit axis 0; every judged run returned `n<i>` above
/// `p<i>`.
async fn embedded_graph(h: &Harness) -> Arc<eg_core::graph::GraphCore> {
    let mut guard = h.state.write().await;
    guard
        .registry
        .create_graph(GRAPH, crate::protocol::GraphType::Team, None)
        .unwrap();
    let core = guard.registry.get(GRAPH).unwrap().core.clone();
    let space =
        eg_types::embedding::EmbeddingSpaceRef::pinned("m", "1", "sha256:w", "sha256:p", 3, false)
            .unwrap();
    core.semantic_store.write().declare_space(space).unwrap();
    for i in 0..40 {
        for (id, vector) in [
            (format!("p{i}"), vec![0.0, 1.0, 0.0]),
            (format!("n{i}"), vec![1.0, 0.0, 0.0]),
        ] {
            let props = serde_json::json!({"type": "Doc"});
            core.add_node(id.clone(), rmp_serde::to_vec_named(&props).unwrap());
            core.semantic_store
                .write()
                .add_embedding(id, vector)
                .unwrap();
        }
    }
    core
}

fn q16(values: &[f64]) -> BoundedVec<i32, 4096> {
    BoundedVec::new(
        values
            .iter()
            .map(|v| (v * 65_536.0).round() as i32)
            .collect(),
    )
    .unwrap()
}

async fn judged_runs(h: &Harness, space: &str) {
    let template_entry = executed_template(h).await;
    for i in 0..40 {
        let record_id = format!("rec-{i:02}");
        clone_executed(h, &template_entry, &record_id);
        let (n, p) = (format!("n{i}"), format!("p{i}"));
        let mut attested = outcome(&record_id, &[&n, &p], &[&p]);
        attested.query = Some(QueryVector {
            space_digest: space.to_string(),
            q16: q16(&[1.0, 0.9 + f64::from(i) * 0.001, 0.0]),
        });
        let recorded = learn(
            h,
            "decider",
            RetrievalOp::RecordOutcome {
                outcome: Box::new(attested),
            },
        )
        .await;
        assert!(recorded.is_ok(), "{recorded:?}");
        evaluate_by(h, "evaluator", &record_id, true).await;
    }
}

fn fit_request(space: &str) -> AdapterFitRequest {
    AdapterFitRequest {
        graph: GRAPH.to_string(),
        space_digest: space.to_string(),
        question_id: None,
        window: window(),
        rank: 2,
        max_gain_q16: 1 << 15,
        holdout_per_mille: 300,
        min_eval_items: 5,
    }
}

async fn adapter_state(h: &Harness, retrieval: RetrievalOp) -> Result<AdapterState, String> {
    match learn(h, "decider", retrieval).await? {
        RetrievalResult::Adapter(state) => Ok(*state),
        other => panic!("adapter state: {other:?}"),
    }
}

fn top(core: &eg_core::graph::GraphCore, query: &[f32]) -> String {
    core.semantic_store.read().semantic_search(query, 1)[0]
        .0
        .clone()
}

#[tokio::test]
async fn an_adapter_is_served_only_after_its_passing_receipt_and_rolls_back() {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    let core = embedded_graph(&h).await;
    let space = core.semantic_store.read().space().unwrap().digest.clone();
    judged_runs(&h, &space).await;

    let fitted: AdapterFitted = match learn(
        &h,
        "decider",
        RetrievalOp::FitAdapter {
            request: Box::new(fit_request(&space)),
        },
    )
    .await
    .unwrap()
    {
        RetrievalResult::Fitted(fitted) => *fitted,
        other => panic!("fit: {other:?}"),
    };
    let receipt = &fitted.receipt;
    assert!(receipt.passed, "{receipt:?}");
    assert!(receipt.adapted_mrr_q16 > receipt.base_mrr_q16);
    assert_eq!(receipt.n_training + receipt.n_eval, 40);

    let tenant = crate::server::access::CarrierAuthority::from_verified(&verified())
        .unwrap()
        .tenant_scope()
        .to_string();
    let served = || super::super::served_adapter::served_for(&h.state, Some(&tenant), &core);
    let query = [1.0_f32, 0.9, 0.0];
    assert!(served().await.is_none(), "fitted is not active");
    assert!(
        top(&core, &query).starts_with('n'),
        "the base query prefers the negatives"
    );

    let activate = |receipt_digest: String| RetrievalOp::ActivateAdapter {
        space_digest: space.clone(),
        adapter_digest: fitted.adapter_digest.clone(),
        receipt_digest,
    };
    let wrong = adapter_state(&h, activate(format!("sha256:{}", "0".repeat(64)))).await;
    assert!(wrong
        .unwrap_err()
        .starts_with("EVALUATION_RECEIPT_MISMATCH"));
    let active = adapter_state(&h, activate(fitted.receipt_digest.clone()))
        .await
        .unwrap();
    assert_eq!(
        active.active.and_then(|e| e.adapter_digest),
        Some(fitted.adapter_digest.clone())
    );
    let adapter = served().await.expect("the activated adapter serves");
    let adapted = adapter.adapt(&query).unwrap();
    assert!(
        top(&core, &adapted).starts_with('p'),
        "the adapter re-aims toward what was cited"
    );

    let rolled = adapter_state(
        &h,
        RetrievalOp::RollbackAdapter {
            space_digest: space.clone(),
        },
    )
    .await
    .unwrap();
    assert!(rolled.active.is_none());
    assert_eq!(rolled.history.len(), 2, "both moves are audited");
    assert!(
        served().await.is_none(),
        "rollback serves the base query again"
    );
    let again = adapter_state(
        &h,
        RetrievalOp::RollbackAdapter {
            space_digest: space,
        },
    )
    .await;
    assert!(again.unwrap_err().starts_with("PARAMETER_INVALID"));
}

/// A unified plan's vector ranks (nested ones included) are re-aimed; the
/// rest of the plan is untouched.
#[cfg(feature = "query")]
#[test]
fn a_plan_rewrite_re_aims_every_vector_rank_only() {
    use eg_numeric::decision::adapter::{to_body, Direction};
    let body = to_body(
        &[Direction {
            unit: vec![0.0, 1.0],
            gain: 0.5,
        }],
        "sha256:s",
        2,
        ("sha256:t".to_string(), 1),
    )
    .unwrap();
    let adapter = super::super::served_adapter::ServedAdapter::for_body("d", &body).unwrap();
    let plan = eg_plan::Plan::new(vec![
        eg_plan::Op::Scan {
            label: "Doc".to_string(),
        },
        eg_plan::Op::FuseRrf {
            branches: vec![vec![eg_plan::Op::Rank {
                query: vec![1.0, 1.0],
            }]],
            k: 60.0,
        },
        eg_plan::Op::Rank {
            query: vec![1.0, 1.0],
        },
    ]);
    let rewritten = super::super::served_adapter::adapt_plan(plan, Some(Arc::new(adapter)));
    let expected = eg_plan::Op::Rank {
        query: vec![1.0, 1.5],
    };
    assert_eq!(rewritten.ops[2], expected);
    let eg_plan::Op::FuseRrf { branches, .. } = &rewritten.ops[1] else {
        panic!("fuse")
    };
    assert_eq!(branches[0][0], expected);
    assert!(matches!(rewritten.ops[0], eg_plan::Op::Scan { .. }));
}
