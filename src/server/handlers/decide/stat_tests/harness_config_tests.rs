//! The published harness kind must fail closed until its executor is served.

use super::*;
use crate::protocol::{Method, Request, Response};

async fn dispatch(h: &Harness, method: Method) -> Response {
    let method = rmp_serde::from_slice(&rmp_serde::to_vec_named(&method).unwrap()).unwrap();
    Box::pin(crate::server::dispatch::dispatch_verified_request(
        &h.state,
        Request {
            id: 128,
            graph: "__commons__".into(),
            auth_token: String::new(),
            agent_id: Some("decider".into()),
            method,
        },
        VerifiedRequestContext::verified_for_test_with_scopes(
            "decider",
            TENANT,
            &["kg:read", "agent:decision-write"],
        ),
    ))
    .await
}

async fn route_fixture() -> (Harness, DecideRequest) {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    h.publish(
        "tool-search",
        AgentComponentKind::Tool,
        "web search engine",
        None::<&()>,
        None,
    )
    .unwrap();
    let (schema_pin, _) = publish_route_schema(&h);
    let asked = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    (h, asked)
}

fn assert_unsupported(response: Response) {
    assert!(response.result.is_none(), "{response:?}");
    assert_eq!(response.error.as_deref(), Some("PARAMETER_INVALID"));
    assert!(
        response.error_detail.as_deref().is_some_and(|detail| {
            detail.contains("harness_config") && detail.contains("not supported")
        }),
        "{response:?}"
    );
}

// spec: EG-DECISION-ENGINE-R128.1
#[tokio::test]
async fn harness_config_dispatch_refuses_generic_scoring() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (h, mut asked) = route_fixture().await;
    let baseline: DecisionBatch = decode(
        dispatch(
            &h,
            Method::Decide {
                request: Box::new(asked.clone()),
            },
        )
        .await,
    )
    .unwrap();
    assert_eq!(
        baseline.records.as_slice()[0].question.kind,
        QuestionKind::Route
    );
    asked.question.kind = QuestionKind::HarnessConfig;
    assert_unsupported(
        dispatch(
            &h,
            Method::Decide {
                request: Box::new(asked.clone()),
            },
        )
        .await,
    );
    asked.tenant_id = "tenant-other".into();
    let foreign = dispatch(
        &h,
        Method::Decide {
            request: Box::new(asked),
        },
    )
    .await;
    assert_eq!(foreign.error.as_deref(), Some("ACCESS_DENIED"));
    assert!(foreign.result.is_none());
}

// spec: EG-DECISION-ENGINE-R128.1
// spec: EG-DECISION-ENGINE-R128.13.1
#[tokio::test]
async fn harness_config_replay_refuses_a_relabelled_generic_record() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (h, asked) = route_fixture().await;
    let baseline: DecisionBatch = decode(
        dispatch(
            &h,
            Method::Decide {
                request: Box::new(asked),
            },
        )
        .await,
    )
    .unwrap();
    let record = baseline.records.as_slice()[0].clone();
    let committed_response = dispatch(
        &h,
        Method::DecisionLog {
            op: Box::new(DecisionLogOp::Commit {
                record: Box::new(record.clone()),
                evaluator: None,
            }),
        },
    )
    .await;
    assert!(
        committed_response.error.is_none(),
        "authorized Route DecisionLog control: {committed_response:?}"
    );
    let committed: DecisionLogCommitted = decode(committed_response).unwrap();
    assert_eq!(committed.record_id, record.record_id);
    for evidence in [false, true] {
        let mut relabelled = record.clone();
        if evidence {
            relabelled.inputs.harness_evaluation = Some(unverified_harness_evidence());
        } else {
            relabelled.question.kind = QuestionKind::HarnessConfig;
        }
        relabelled.inputs_digest = eg_types::decision::digest::digest_text(
            super::super::stat_decide::STATISTICAL_INPUTS_DOMAIN,
            &(&relabelled.question, &relabelled.inputs),
        );
        relabelled.record_id = format!(
            "decision:{}",
            relabelled.inputs_digest.trim_start_matches("sha256:"),
        );
        relabelled.record_digest =
            eg_types::decision::digest::statistical_record_digest(&relabelled);
        assert_unsupported(
            dispatch(
                &h,
                Method::DecisionLog {
                    op: Box::new(DecisionLogOp::Commit {
                        record: Box::new(relabelled.clone()),
                        evaluator: None,
                    }),
                },
            )
            .await,
        );
        assert!(h
            .store
            .decision_artifact(
                TENANT,
                &crate::server::persistence::decision_jobs::record_key(&relabelled.record_id),
            )
            .unwrap()
            .is_none());
    }
}

fn unverified_harness_evidence(
) -> eg_types::decision::statistical::harness_config::HarnessConfigEvaluationEvidence {
    let missing = |value: serde_json::Value| {
        serde_json::json!({
            "status": "abstained", "missing_fact": ["unverified"],
            "conservative_default": value
        })
    };
    serde_json::from_value(serde_json::json!({
        "model_profile": {"component_id": "profile", "kind": "model_profile",
            "definition_digest": "sha256:unverified"},
        "request": {"model_profile_ref": "profile", "task_class": "task",
            "usable_context_budget_tokens": 10, "priority": "interactive"},
        "answer": {
            "context_policy": missing(serde_json::json!("elide_then_summarize")),
            "soft_threshold": missing(serde_json::json!({"numerator": 1, "denominator": 4294967296u64})),
            "hard_threshold": missing(serde_json::json!({"numerator": 2, "denominator": 4294967296u64})),
            "recent_window_fraction": missing(serde_json::json!({"numerator": 1, "denominator": 4294967296u64})),
            "plan_scaffold": missing(serde_json::json!("on")),
            "action_space": missing(serde_json::json!("tool_set")),
            "tool_subset": missing(serde_json::json!([]))
        }
    })).unwrap()
}
