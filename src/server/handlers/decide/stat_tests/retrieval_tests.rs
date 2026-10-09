//! Retrieval outcomes on the decision log (EH-394, EH-395, EH-398), served:
//! outcomes join only their committer's executed retrieval-plan records,
//! nothing is learned before an independent verdict, and what was learned is
//! read only through the decision views' reserved SQL relations.

use super::consumer_tests::declared_abstention;
use super::*;
use eg_types::decision::statistical::retrieval::{
    LearningRecorded, LearningWrite, PathEdge, PathRank, RetrievalOutcome, RetrievalPathTemplate,
    ReturnedEvidence,
};
use serde_json::{json, Value};

pub(super) async fn learn(
    h: &Harness,
    who: &str,
    write: LearningWrite,
) -> Result<LearningRecorded, String> {
    let op = DecisionLogOp::Learn {
        tenant_id: TENANT.to_string(),
        write,
    };
    decode(log_op(h, who, op).await)
}

pub(super) fn attest(outcome: RetrievalOutcome) -> LearningWrite {
    LearningWrite::RecordOutcome {
        outcome: Box::new(outcome),
    }
}

/// One SQL statement over `who`'s authorized projection (the decision views
/// included), as every served SQL surface reads it.
pub(super) async fn sql(h: &Harness, who: &str, query: &str) -> Vec<Vec<Value>> {
    use crate::server::access::CarrierAuthority;
    use crate::server::sql_catalog_acl::authorized_read_store_for_query;
    let verified = VerifiedRequestContext::verified_for_test_in_tenant(who, TENANT);
    let authority = CarrierAuthority::from_verified(&verified).unwrap();
    let persist_dir = std::path::PathBuf::from(h.state.read().await.persist_dir.clone().unwrap());
    let views = super::super::stat_view::DecisionViews::of(h.store.clone(), &authority);
    let graph = crate::graph::GraphCore::new().analysis_snapshot();
    let request_graph = crate::server::sql_catalog_acl::RequestGraph {
        name: "retrieval-sql-graph".to_string(),
        core: std::sync::Arc::new(crate::graph::GraphCore::new()),
    };
    // The SQL leg drives its own runtime (as on the blocking pool in
    // production), so it runs on a plain thread outside the test's runtime.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let projection = authorized_read_store_for_query(
                    &authority,
                    &persist_dir,
                    query,
                    Some(&views),
                    &request_graph,
                )
                .unwrap();
                eg_query::exec_sql_typed_with_tables(&graph, projection.store(), query)
                    .unwrap()
                    .rows
            })
            .join()
            .unwrap()
    })
}

/// A committed retrieval-plan record that EXECUTED `plan-hyde`, cloned under
/// `record_id`: the logged declared abstention with its outcome set to an
/// exploration (the log join reads the stored entry; it does not replay).
pub(super) async fn executed_template(h: &Harness) -> DecisionLogEntry {
    let record = declared_abstention(h).await;
    let commit = DecisionLogOp::Commit {
        record: Box::new(record),
        evaluator: None,
    };
    let logged: DecisionLogCommitted = decode(log_op(h, "decider", commit).await).unwrap();
    let key = crate::server::persistence::decision_jobs::record_key(&logged.record_id);
    let bytes = h.store.decision_artifact(TENANT, &key).unwrap().unwrap();
    crate::server::persistence::decision_jobs::decode_artifact(&bytes, "entry").unwrap()
}

/// The persistence id of test principal `who`.
pub(super) fn principal_of(who: &str) -> String {
    VerifiedRequestContext::verified_for_test_in_tenant(who, TENANT).principal_persistence_id()
}

/// The template record, EXECUTED, under `record_id`, keeping its
/// committer-only visibility, with a live grant naming `evaluator` (EH-395).
pub(super) fn clone_executed(h: &Harness, template: &DecisionLogEntry, record_id: &str) {
    clone_record(h, template, record_id);
    let now = crate::server::dispatch::authoritative_now_ms();
    grant(h, record_id, "evaluator", (now, now + 3_600_000));
}

/// Write the committer's named-evaluator grant for `record_id`, issued and
/// expiring at `span` (as `DecisionLog.commit` writes it).
pub(super) fn grant(h: &Harness, record_id: &str, evaluator: &str, span: (u64, u64)) {
    let ctx = super::super::stat_executor::ExecutionContext {
        store: &h.store,
        tenant_id: TENANT,
        now_ms: span.0,
        server_secret: b"",
    };
    let named = eg_types::decision::statistical::log::NamedEvaluator {
        principal: Some(principal_of(evaluator)),
        role: None,
        expires_at_ms: span.1,
    };
    let committer = principal_of("decider");
    let rows = super::super::stat_evaluator::grant_rows(&ctx, record_id, &committer, Some(&named))
        .unwrap();
    h.store.put_decision_artifacts(TENANT, &rows).unwrap();
}

/// The template record, EXECUTED, under `record_id`, with no grant.
pub(super) fn clone_record(h: &Harness, template: &DecisionLogEntry, record_id: &str) {
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

fn returned_unclassed(ids: &[&str]) -> BoundedVec<ReturnedEvidence, 256> {
    BoundedVec::new(
        ids.iter()
            .map(|id| ReturnedEvidence {
                evidence_id: id.to_string(),
                content_class: None,
            })
            .collect(),
    )
    .unwrap()
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

pub(super) fn outcome(record_id: &str, ids: &[&str], cited: &[&str]) -> RetrievalOutcome {
    RetrievalOutcome {
        record_id: record_id.to_string(),
        returned: returned(ids, "doc"),
        cited: BoundedVec::new(cited.iter().map(|c| c.to_string()).collect()).unwrap(),
        query: None,
        path: None,
    }
}

pub(super) async fn evaluate_by(h: &Harness, who: &str, record_id: &str, success: bool) {
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

/// A record only its committer may see, attested: nothing learned from it
/// reaches anyone else.
async fn attest_private(h: &Harness, template_entry: &DecisionLogEntry) {
    clone_record(h, template_entry, "rec-private");
    let mut private = outcome("rec-private", &["x"], &[]);
    private.returned = returned_unclassed(&["x"]);
    assert!(learn(h, "decider", attest(private)).await.is_ok());
}

// spec: EG-DECISION-ENGINE-R100
#[tokio::test]
async fn outcomes_teach_nothing_until_an_independent_verdict_joins_them() {
    let h = Harness::new().await;
    let template_entry = executed_template(&h).await;
    clone_executed(&h, &template_entry, "rec-1");
    let units: Vec<String> = (0..12).map(|i| format!("e{i}")).collect();
    let ids: Vec<&str> = units.iter().map(String::as_str).collect();
    let mut attested = outcome("rec-1", &ids, &["e5"]);
    attested.path = Some(template("sha256:schema-a"));

    let foreign = learn(&h, "stranger", attest(attested.clone())).await;
    assert!(
        foreign.unwrap_err().starts_with("PARAMETER_INVALID"),
        "a stranger cannot even see the record"
    );
    attest_private(&h, &template_entry).await;
    let mut bogus = attested.clone();
    bogus.cited = BoundedVec::new(vec!["zz".to_string()]).unwrap();
    assert!(learn(&h, "decider", attest(bogus)).await.is_err());
    let recorded = learn(&h, "decider", attest(attested.clone()))
        .await
        .unwrap();
    assert!(matches!(recorded, LearningRecorded::Outcome(_)));
    let mut conflicting = attested.clone();
    conflicting.cited = BoundedVec::new(vec!["e6".to_string()]).unwrap();
    let conflict = learn(&h, "decider", attest(conflicting)).await;
    assert!(conflict.unwrap_err().starts_with("IDEMPOTENCY_CONFLICT"));

    let negatives = "SELECT evidence_id, rank FROM decision_hard_negatives ORDER BY rank";
    let verdicts =
        "SELECT DISTINCT verdict FROM decision_retrieval_outcomes WHERE record_id = 'rec-1'";
    assert_eq!(
        sql(&h, "decider", verdicts).await,
        vec![vec![json!("unjudged")]]
    );
    assert!(sql(&h, "decider", negatives).await.is_empty());

    evaluate_by(&h, "decider", "rec-1", true).await;
    assert!(
        sql(&h, "decider", negatives).await.is_empty(),
        "the attester's own verdict is never a label"
    );
    evaluate_by(&h, "evaluator", "rec-1", true).await;
    let rows = sql(&h, "decider", negatives).await;
    let expected: Vec<Vec<Value>> = (0..5)
        .map(|i| vec![json!(format!("e{i}")), json!(i + 1)])
        .collect();
    assert_eq!(rows, expected);
    let private =
        "SELECT count(*) FROM decision_retrieval_outcomes WHERE record_id = 'rec-private'";
    assert_eq!(sql(&h, "decider", private).await, vec![vec![json!(1)]]);
    assert_eq!(
        sql(&h, "stranger", private).await,
        vec![vec![json!(0)]],
        "another principal's record is not even counted"
    );
    paths_and_usage_follow_the_verdict(&h).await;
}

/// After the independent success: the path is proven under its schema identity,
/// and the per-class usage is k-anonymised below `min_support`.
async fn paths_and_usage_follow_the_verdict(h: &Harness) {
    let proven = sql(
        h,
        "decider",
        "SELECT composed_digest, successes, failures, skill_ref FROM decision_proven_paths",
    )
    .await;
    assert_eq!(
        proven,
        vec![vec![
            json!("sha256:schema-a"),
            json!(1),
            json!(0),
            json!("skill.incident-triage")
        ]]
    );
    let usage = sql(
        h,
        "decider",
        "SELECT content_class, returned, cited FROM decision_class_usage",
    )
    .await;
    assert_eq!(
        usage,
        vec![vec![json!("doc"), json!(12), json!(1)]],
        "12 returned units clear the default min_support of 10"
    );
    let cited = sql(
        h,
        "decider",
        "SELECT evidence_id FROM decision_retrieval_outcomes WHERE cited",
    )
    .await;
    assert_eq!(cited, vec![vec![json!("e5")]]);
}
