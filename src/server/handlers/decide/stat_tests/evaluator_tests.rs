//! Named evaluators (EH-395): the committer names the one principal -- or the
//! one declared policy role -- that may evaluate its committer-only record;
//! that grant is record-scoped, expiring and evaluation-only, and nothing else
//! widens.

use super::consumer_tests::declared_abstention;
use super::retrieval_tests::{clone_record, executed_template, grant, principal_of, sql};
use super::*;
use eg_types::decision::statistical::log::NamedEvaluator;
use serde_json::json;

const ROLE: &str = "decide-evaluator";

fn named(principal: Option<String>, role: Option<&str>) -> NamedEvaluator {
    let now = crate::server::dispatch::authoritative_now_ms();
    NamedEvaluator {
        principal,
        role: role.map(str::to_string),
        expires_at_ms: now + 3_600_000,
    }
}

fn commit_with(record: &StatisticalDecisionRecord, evaluator: NamedEvaluator) -> DecisionLogOp {
    DecisionLogOp::Commit {
        record: Box::new(record.clone()),
        evaluator: Some(evaluator),
    }
}

fn commit_naming(record: &StatisticalDecisionRecord, evaluator: &str) -> DecisionLogOp {
    commit_with(record, named(Some(principal_of(evaluator)), None))
}

async fn commit_as_decider(
    h: &Harness,
    record: &StatisticalDecisionRecord,
    evaluator: NamedEvaluator,
) -> Result<DecisionLogCommitted, String> {
    decode(log_op(h, "decider", commit_with(record, evaluator)).await)
}

/// `who`, verified with `roles` in its signed request context.
fn holding(who: &str, roles: &[&str]) -> VerifiedRequestContext {
    VerifiedRequestContext::from_verified_claims(
        crate::acl::RequestContextClaims {
            principal: format!("principal:{who}"),
            tenant: TENANT.to_string(),
            agent_id: who.to_string(),
            audience: "epistemic-graph".to_string(),
            policy_version: "policy-test".to_string(),
            scopes: vec!["kg:read".to_string()],
            roles: roles.iter().map(|r| r.to_string()).collect(),
            ..crate::acl::RequestContextClaims::default()
        },
        format!("test:{who}"),
    )
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

async fn evaluate_holding(
    h: &Harness,
    who: &str,
    roles: &[&str],
    record_id: &str,
) -> Result<StoredEvaluation, String> {
    let op = evaluation_of(record_id, &format!("eval-{who}"));
    let verified = holding(who, roles);
    decode(super::super::log::handle_decision_log(&h.state, 9, &verified, op).await)
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

#[tokio::test]
async fn a_commit_names_exactly_one_principal_or_one_plain_role() {
    let h = Harness::new().await;
    let record = declared_abstention(&h).await;
    let malformed = [
        named(Some(principal_of("evaluator")), Some(ROLE)),
        named(None, None),
        named(None, Some("*")),
    ];
    for evaluator in malformed {
        let refused = commit_as_decider(&h, &record, evaluator).await;
        assert!(refused.unwrap_err().starts_with("PARAMETER_INVALID"));
    }
    let logged = commit_as_decider(&h, &record, named(None, Some(ROLE)))
        .await
        .unwrap();
    let key = format!("evaluator-lease:{}", logged.record_id);
    assert!(h.store.decision_artifact(TENANT, &key).unwrap().is_some());
}

#[tokio::test]
async fn a_role_grant_admits_its_holders_but_never_the_committer() {
    let h = Harness::new().await;
    let template = executed_template(&h).await;
    clone_record(&h, &template, "rec-r");
    let ctx = super::super::stat_executor::ExecutionContext {
        store: &h.store,
        tenant_id: TENANT,
        now_ms: crate::server::dispatch::authoritative_now_ms(),
        server_secret: b"",
    };
    let committer = principal_of("decider");
    let rows = super::super::stat_evaluator::grant_rows(
        &ctx,
        "rec-r",
        &committer,
        Some(&named(None, Some(ROLE))),
    )
    .unwrap();
    h.store.put_decision_artifacts(TENANT, &rows).unwrap();

    let outsider = evaluate_holding(&h, "stranger", &["reader"], "rec-r").await;
    assert!(outsider.unwrap_err().starts_with("PARAMETER_INVALID"));
    let own = evaluate_holding(&h, "decider", &[ROLE], "rec-r").await;
    assert!(own.is_ok(), "the committer sees its own record");
    let stored = evaluate_holding(&h, "judge", &[ROLE], "rec-r")
        .await
        .unwrap();
    assert_eq!(stored.producer, principal_of("judge"));
    let get = DecisionLogOp::Get {
        tenant_id: TENANT.to_string(),
        record_id: "rec-r".to_string(),
    };
    let verified = holding("judge", &[ROLE]);
    let seen: Option<DecisionLogEntry> =
        decode(super::super::log::handle_decision_log(&h.state, 9, &verified, get).await).unwrap();
    assert!(seen.is_none(), "a role grant is evaluation-only: no read");
}
