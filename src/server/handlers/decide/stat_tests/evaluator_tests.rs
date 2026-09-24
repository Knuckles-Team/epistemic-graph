//! Named evaluators (EH-395): the committer names the one principal that may
//! evaluate its committer-only record; that grant is record-scoped, expiring
//! and evaluation-only, and nothing else widens.

use super::consumer_tests::declared_abstention;
use super::retrieval_tests::{clone_record, executed_template, grant, principal_of, sql};
use super::*;
use eg_types::decision::statistical::log::NamedEvaluator;
use serde_json::json;

fn commit_naming(record: &StatisticalDecisionRecord, evaluator: &str) -> DecisionLogOp {
    let now = crate::server::dispatch::authoritative_now_ms();
    DecisionLogOp::Commit {
        record: Box::new(record.clone()),
        evaluator: Some(NamedEvaluator {
            principal: principal_of(evaluator),
            expires_at_ms: now + 3_600_000,
        }),
    }
}

fn evaluation_of(record_id: &str, id: &str) -> DecisionLogOp {
    DecisionLogOp::Evaluate {
        tenant_id: TENANT.to_string(),
        evaluation: DecisionOutcomeEvaluation {
            record_id: record_id.to_string(),
            evaluation_id: id.to_string(),
            class: EvidenceClass::Observation,
            selected_agent: "retriever-agent".to_string(),
            lease_holder: "retriever-worker".to_string(),
            fidelity: OutcomeFidelity::ToolCalls,
            success: Some(true),
        },
    }
}

async fn evaluate_as(h: &Harness, who: &str, record_id: &str) -> Result<StoredEvaluation, String> {
    decode(log_op(h, who, evaluation_of(record_id, &format!("eval-{who}"))).await)
}

#[tokio::test]
async fn a_commit_names_its_evaluator_and_never_itself() {
    let h = Harness::new().await;
    let record = declared_abstention(&h).await;
    let own = decode::<DecisionLogCommitted>(
        log_op(&h, "decider", commit_naming(&record, "decider")).await,
    );
    assert!(
        own.unwrap_err().starts_with("PARAMETER_INVALID"),
        "self-evaluation is never independent"
    );
    let logged: DecisionLogCommitted =
        decode(log_op(&h, "decider", commit_naming(&record, "evaluator")).await).unwrap();
    let key = format!("evaluator-lease:{}", logged.record_id);
    let lease = h.store.decision_artifact(TENANT, &key).unwrap();
    assert!(lease.is_some(), "the grant is written with the record");
}

#[tokio::test]
async fn only_the_named_evaluator_evaluates_and_nothing_else_widens() {
    let h = Harness::new().await;
    let template = executed_template(&h).await;
    let now = crate::server::dispatch::authoritative_now_ms();
    clone_record(&h, &template, "rec-g");
    grant(&h, "rec-g", "evaluator", (now, now + 3_600_000));

    assert!(evaluate_as(&h, "evaluator", "rec-g").await.is_ok());
    let stranger = evaluate_as(&h, "stranger", "rec-g").await;
    assert!(stranger.unwrap_err().starts_with("PARAMETER_INVALID"));

    let get = DecisionLogOp::Get {
        tenant_id: TENANT.to_string(),
        record_id: "rec-g".to_string(),
    };
    let seen: Option<DecisionLogEntry> = decode(log_op(&h, "evaluator", get).await).unwrap();
    assert!(seen.is_none(), "the grant is evaluation-only: no read");
    let count = "SELECT count(*) FROM decisions WHERE record_id = 'rec-g'";
    assert_eq!(sql(&h, "evaluator", count).await, vec![vec![json!(0)]]);
    assert_eq!(sql(&h, "stranger", count).await, vec![vec![json!(0)]]);
    assert_eq!(sql(&h, "decider", count).await, vec![vec![json!(1)]]);
    let joined = "SELECT producer FROM decision_evaluations WHERE record_id = 'rec-g'";
    assert_eq!(
        sql(&h, "decider", joined).await,
        vec![vec![json!(principal_of("evaluator"))]],
        "the committer sees the independent evaluation"
    );
}

#[tokio::test]
async fn an_expired_grant_is_refused() {
    let h = Harness::new().await;
    let template = executed_template(&h).await;
    let now = crate::server::dispatch::authoritative_now_ms();
    clone_record(&h, &template, "rec-x");
    grant(&h, "rec-x", "evaluator", (now - 7_200_000, now - 3_600_000));
    let expired = evaluate_as(&h, "evaluator", "rec-x").await;
    assert!(expired.unwrap_err().starts_with("PARAMETER_INVALID"));
}
