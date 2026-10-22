//! Bounded explicit plans through the actual Decide handler.

use super::*;
use eg_types::decision::statistical::declared::DeclaredOption;
use eg_types::decision::task_plan::{TaskPlanProposal, TaskPlanTask, TASK_PLAN_POLICY_REVISION};

fn task_request(pin: &ComponentDependency) -> DecideRequest {
    let tasks: Vec<TaskPlanTask> = [
        ("a", vec![]),
        ("b", vec![]),
        ("c", vec!["b"]),
        ("join", vec!["a", "c"]),
    ]
    .into_iter()
    .map(|(id, deps)| TaskPlanTask {
        task_id: id.into(),
        prompt: format!("Analyze {id} with evidence"),
        depends_on: BoundedVec::new(deps.into_iter().map(str::to_string).collect()).unwrap(),
    })
    .collect();
    DecideRequest {
        tenant_id: TENANT.into(),
        question: StatisticalQuestion {
            question_id: "explicit-plan".into(),
            kind: QuestionKind::TaskPlan,
            safety: QuestionSafety::Ordinary,
        },
        candidates: CandidateSource::Declared {
            options: BoundedVec::new(
                tasks
                    .iter()
                    .map(|t| DeclaredOption {
                        option_id: t.task_id.clone(),
                        classification: BoundedVec::default(),
                        numbers: BoundedVec::default(),
                        texts: BoundedVec::default(),
                    })
                    .collect(),
            )
            .unwrap(),
        },
        feature_schema: pin.clone(),
        head: None,
        policy: DecisionPolicyRef::Default,
        params: BoundedVec::new(vec![TypedParam {
            name: "task_plan".into(),
            value: TypedValue::TaskPlan(Box::new(TaskPlanProposal {
                tasks: BoundedVec::new(tasks).unwrap(),
                output_task_id: "join".into(),
                max_concurrency: 2,
                max_steps: 4,
            })),
        }])
        .unwrap(),
        max_records: None,
        belief_as_of: BoundedVec::default(),
    }
}

// spec: EG-DECISION-ENGINE-R134.111.3
// spec: EG-DECISION-ENGINE-R134.111.5
#[tokio::test]
async fn decide_routes_task_plan() {
    let h = Harness::new().await;
    let (pin, _) = publish_route_schema(&h);
    let batch = decide(&h, task_request(&pin)).await.unwrap();
    let record = &batch.records.as_slice()[0];
    let plan = record.task_plan.as_ref().unwrap();
    assert_eq!(
        plan.tasks
            .iter()
            .map(|t| t.task_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c", "join"]
    );
    assert_eq!(plan.policy_revision, TASK_PLAN_POLICY_REVISION);
    assert_eq!(record.evidence_class, EvidenceClass::Claim);
    assert_eq!(
        record.resolution_kind,
        eg_types::decision::ResolutionKind::Constraint
    );
    assert_eq!(record.tenant_id, TENANT);
    assert_eq!(
        record.caller_principal,
        verified().principal_persistence_id()
    );
    assert!(record.premises.iter().any(|p| p.subject == "c"
        && p.fact.contains("dependencies=b")
        && p.class == eg_types::decision::PremiseClass::Claim));
    assert!(record.premises.iter().any(|p|p.subject==TASK_PLAN_POLICY_REVISION && matches!(&p.provenance, eg_types::decision::PremiseProvenance::Policy {policy_digest} if *policy_digest==plan.policy_digest)));
    assert_eq!(
        eg_types::decision::digest::statistical_record_digest(record),
        record.record_digest
    );
    let mut altered = record.clone();
    altered.task_plan.as_mut().unwrap().max_concurrency = 3;
    assert_ne!(
        eg_types::decision::digest::statistical_record_digest(&altered),
        record.record_digest
    );
}

async fn assert_invalid(h: &Harness, request: DecideRequest, detail: &str) {
    let response =
        super::super::statistical::handle_decide(&h.state, 1, &verified(), request).await;
    assert_eq!(response.error.as_deref(), Some("PARAMETER_INVALID"));
    assert!(
        response
            .error_detail
            .as_deref()
            .is_some_and(|text| text.contains(detail)),
        "expected {detail:?}, got {:?}",
        response.error_detail
    );
}

// spec: EG-DECISION-ENGINE-R134.111.4
#[tokio::test]
async fn invalid_task_requests_refuse() {
    let h = Harness::new().await;
    let (pin, _) = publish_route_schema(&h);
    let mut request = task_request(&pin);
    request.question.safety = QuestionSafety::Security;
    assert_invalid(&h, request, "ordinary safety").await;
    let mut request = task_request(&pin);
    request.tenant_id = "foreign-tenant".into();
    assert!(decide(&h, request)
        .await
        .unwrap_err()
        .starts_with("ACCESS_DENIED"));
    let mut request = task_request(&pin);
    request.head = Some(pin.clone());
    assert_invalid(&h, request, "no head").await;
    let mut request = task_request(&pin);
    request.belief_as_of = BoundedVec::new(vec![1]).unwrap();
    assert_invalid(&h, request, "belief slices").await;
    let mut request = task_request(&pin);
    request.candidates = CandidateSource::Declared {
        options: BoundedVec::default(),
    };
    assert_invalid(&h, request, "exactly the sorted task ids").await;
    let mut request = task_request(&pin);
    request.question.kind = QuestionKind::Route;
    assert_invalid(&h, request, "requires task_plan question").await;
    let mut request = task_request(&pin);
    let mut params = request.params.as_slice().to_vec();
    let TypedValue::TaskPlan(proposal) = &mut params[0].value else {
        unreachable!()
    };
    proposal.output_task_id = "b".into();
    request.params = BoundedVec::new(params).unwrap();
    assert_invalid(&h, request, "disconnected_output").await;
}

// spec: EG-DECISION-ENGINE-R134.111.5
#[tokio::test]
async fn task_plan_cannot_use_score_replay() {
    let h = Harness::new().await;
    let (pin, _) = publish_route_schema(&h);
    let batch = decide(&h, task_request(&pin)).await.unwrap();
    let mut record = batch.records.as_slice()[0].clone();
    record.question.kind = QuestionKind::Route;
    record.outcome = StatisticalOutcome::Abstained {
        reasons: BoundedVec::default(),
    };
    record.record_digest = eg_types::decision::digest::statistical_record_digest(&record);
    let ctx = super::super::stat_executor::ExecutionContext {
        store: h.store.as_ref(),
        tenant_id: TENANT,
        now_ms: 1,
        server_secret: b"fixture",
    };
    assert!(super::super::stat_replay::replay(&ctx, &record)
        .unwrap_err()
        .contains("evaluate-only"));
}
