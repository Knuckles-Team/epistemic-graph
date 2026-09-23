//! The consumer surface (decide-consumers lane): caller-declared options
//! decide as claims under the declaring principal's visibility, and a logged
//! abstention takes an escalation's resolution as evidence of its own class
//! (EH-037) without ever gaining an option it did not hold.

use super::*;
use eg_types::decision::statistical::declared::{DeclaredNumber, DeclaredOption};
use eg_types::decision::statistical::log::{
    AbstentionResolution, AbstentionResolver, StoredResolution,
};

fn declared(id: &str, score: i64) -> DeclaredOption {
    DeclaredOption {
        option_id: id.to_string(),
        classification: BoundedVec::default(),
        numbers: BoundedVec::new(vec![DeclaredNumber {
            key: "score".to_string(),
            q32: score,
        }])
        .unwrap(),
        texts: BoundedVec::default(),
    }
}

fn score_schema() -> FeatureSchemaBody {
    FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: BoundedVec::new(vec![FeatureSpec {
            name: "score".to_string(),
            kind: FeatureKind::Number {
                key: "score".to_string(),
            },
            missing: MissingValue::Abstain,
        }])
        .unwrap(),
    }
}

fn declared_request(schema: &ComponentDependency, options: Vec<DeclaredOption>) -> DecideRequest {
    DecideRequest {
        tenant_id: TENANT.to_string(),
        question: StatisticalQuestion {
            question_id: "au.retrieval-plan".to_string(),
            kind: QuestionKind::RetrievalPlan,
            safety: QuestionSafety::Ordinary,
        },
        candidates: CandidateSource::Declared {
            options: BoundedVec::new(options).unwrap(),
        },
        feature_schema: schema.clone(),
        head: None,
        policy: DecisionPolicyRef::Default,
        params: BoundedVec::default(),
        max_records: None,
    }
}

async fn log_as(h: &Harness, who: &str, op: DecisionLogOp) -> crate::protocol::Response {
    let verified = VerifiedRequestContext::verified_for_test_in_tenant(who, TENANT);
    super::super::log::handle_decision_log(&h.state, 21, &verified, op).await
}

fn resolution(
    record_id: &str,
    id: &str,
    option: &str,
    resolver: AbstentionResolver,
) -> DecisionLogOp {
    DecisionLogOp::Resolve {
        tenant_id: TENANT.to_string(),
        resolution: AbstentionResolution {
            record_id: record_id.to_string(),
            resolution_id: id.to_string(),
            option_id: option.to_string(),
            resolver,
        },
    }
}

async fn resolve(h: &Harness, op: DecisionLogOp) -> Result<StoredResolution, String> {
    decode(log_as(h, "decider", op).await)
}

async fn declared_abstention(h: &Harness) -> StatisticalDecisionRecord {
    let schema_pin = h
        .publish(
            "schema-declared",
            AgentComponentKind::FeatureSchema,
            "declared-option features",
            Some(&score_schema()),
            None,
        )
        .unwrap();
    let options = vec![
        declared("plan-deep", 1 << 31),
        declared("plan-hyde", 1 << 32),
    ];
    let batch = decide(h, declared_request(&schema_pin, options))
        .await
        .unwrap();
    batch.records.as_slice()[0].clone()
}

#[tokio::test]
async fn declared_options_decide_as_claims_under_the_declaring_principal() {
    let h = Harness::new().await;
    let record = declared_abstention(&h).await;
    assert!(matches!(
        record.outcome,
        StatisticalOutcome::Abstained { .. }
    ));
    assert!(matches!(
        record.candidate_source,
        eg_types::decision::CandidateSourceRecord::Declared { .. }
    ));
    assert_eq!(record.evidence_class, EvidenceClass::Claim);
    let claims = record
        .premises
        .iter()
        .filter(|p| p.fact == "declared_option")
        .count();
    assert_eq!(claims, 2, "every declared option is a named claim premise");
    let FeatureMatrixRef::Inline { values, .. } = &record.inputs.feature_matrix else {
        panic!("inline matrix")
    };
    assert_eq!(values.as_slice(), [1 << 31, 1 << 32]);

    let commit = |r: &StatisticalDecisionRecord| DecisionLogOp::Commit {
        record: Box::new(r.clone()),
    };
    let logged: DecisionLogCommitted = decode(log_as(&h, "decider", commit(&record)).await)
        .expect("an abstention is logged after verify-replay");
    let get = DecisionLogOp::Get {
        tenant_id: TENANT.to_string(),
        record_id: logged.record_id.clone(),
    };
    let mine: Option<DecisionLogEntry> = decode(log_as(&h, "decider", get.clone()).await).unwrap();
    assert!(mine.is_some());
    let theirs: Option<DecisionLogEntry> = decode(log_as(&h, "stranger", get).await).unwrap();
    assert!(
        theirs.is_none(),
        "a declared record is the declarer's alone"
    );

    let unsorted = vec![declared("plan-z", 1), declared("plan-a", 2)];
    let refused = decide(
        &h,
        declared_request(&record.inputs.feature_schema, unsorted),
    )
    .await
    .unwrap_err();
    assert!(refused.starts_with("PARAMETER_INVALID"), "{refused}");
}

#[tokio::test]
async fn a_logged_abstention_takes_a_resolution_of_its_resolver_class() {
    let h = Harness::new().await;
    let record = declared_abstention(&h).await;
    let commit = DecisionLogOp::Commit {
        record: Box::new(record.clone()),
    };
    let logged: DecisionLogCommitted = decode(log_as(&h, "decider", commit).await).unwrap();
    let id = logged.record_id.as_str();
    let model = || AbstentionResolver::Model {
        producer: "au-escalation".to_string(),
        prompt_digest: None,
    };

    let foreign = resolve(&h, resolution(id, "r-0", "plan-invented", model())).await;
    assert!(foreign.unwrap_err().starts_with("PARAMETER_INVALID"));

    let claim = resolve(&h, resolution(id, "r-1", "plan-hyde", model()))
        .await
        .unwrap();
    assert_eq!(
        claim.class,
        EvidenceClass::Claim,
        "a model's answer is a claim"
    );
    let again = resolve(&h, resolution(id, "r-1", "plan-hyde", model()))
        .await
        .unwrap();
    assert_eq!(again, claim, "an identical re-send replays");
    let conflict = resolve(&h, resolution(id, "r-1", "plan-deep", model())).await;
    assert!(conflict.unwrap_err().starts_with("IDEMPOTENCY_CONFLICT"));

    let human = resolve(
        &h,
        resolution(id, "r-2", "plan-deep", AbstentionResolver::Human),
    )
    .await
    .unwrap();
    assert_eq!(human.class, EvidenceClass::Observation);

    let evaluation = DecisionLogOp::Evaluate {
        tenant_id: TENANT.to_string(),
        evaluation: DecisionOutcomeEvaluation {
            record_id: id.to_string(),
            evaluation_id: "e-1".to_string(),
            class: EvidenceClass::Observation,
            selected_agent: "agent".to_string(),
            lease_holder: "worker".to_string(),
            fidelity: OutcomeFidelity::ToolCalls,
            success: Some(true),
        },
    };
    let refused = decode::<StoredEvaluation>(log_as(&h, "decider", evaluation).await);
    assert!(
        refused.unwrap_err().contains("executed no option"),
        "an abstention executed nothing an outcome could evaluate"
    );
}
