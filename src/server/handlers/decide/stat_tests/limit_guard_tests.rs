//! EG-DECISION-ENGINE-R131.3 and R131.4: the served statistical `Decide`
//! checks the action it would advise against the policy's limit rules and
//! abstains with `limit_breach` naming the first breached rule.

use super::*;
use eg_types::decision::limit_guard::{
    GuardedAction, LimitMetric, LimitObservation, LimitRule, RequestedBound,
};
use super::tool_subset_tests::{tool, tool_subset_request};
use eg_types::decision::AbstainReason;

fn limited_policy() -> eg_types::decision::DecisionPolicy {
    let mut policy = eg_types::decision::DecisionPolicy::engine_default();
    let mut statistical = super::super::stat_support::default_statistical_policy();
    statistical.limit_rules = BoundedVec::new(vec![
        LimitRule {
            id: "max-position".to_string(),
            metric: LimitMetric::PositionSize,
            bound: 10_000,
            scope: "account:a".to_string(),
        },
        LimitRule {
            id: "max-drawdown".to_string(),
            metric: LimitMetric::Drawdown,
            bound: 500,
            scope: "account:a".to_string(),
        },
    ])
    .unwrap();
    policy.statistical = Some(statistical);
    policy
}

fn action(position: i64, requested: Vec<RequestedBound>) -> GuardedAction {
    GuardedAction {
        graph: "__commons__".to_string(),
        signal_iri: "urn:signal:order-1".to_string(),
        observations: vec![
            LimitObservation {
                metric: LimitMetric::PositionSize,
                scope: "account:a".to_string(),
                value: position,
            },
            LimitObservation {
                metric: LimitMetric::Drawdown,
                scope: "account:a".to_string(),
                value: 100,
            },
        ],
        requested_bounds: requested,
    }
}

fn guarded_request(
    h: &Harness,
    schema: &ComponentDependency,
    guarded: Option<GuardedAction>,
) -> DecideRequest {
    let pin = h.publish_policy("limit-policy", &limited_policy());
    let mut request = tool_subset_request(schema, &["search"], 100, vec![tool("tool-a", &["search"], 10)]);
    request.policy = DecisionPolicyRef::Pinned { component: pin };
    request.guarded_action = guarded;
    request
}

// spec: EG-DECISION-ENGINE-R131.3
// spec: EG-DECISION-ENGINE-R131.4
#[tokio::test]
async fn breach_abstains_with_rule_id() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);

    let breaching = guarded_request(&h, &schema_pin, Some(action(12_000, Vec::new())));
    let batch = decide(&h, breaching).await.unwrap();
    let StatisticalOutcome::Abstained { reasons } = &batch.records.as_slice()[0].outcome else {
        panic!("a breaching action must abstain")
    };
    assert_eq!(
        reasons.as_slice(),
        [AbstainReason::LimitBreach {
            rule_id: "max-position".to_string()
        }]
    );

    // A request bound can only tighten: a tighter bound refuses an action the
    // policy bound allows, a looser one is ignored.
    let tightened = RequestedBound {
        rule_id: "max-position".to_string(),
        bound: 5_000,
    };
    let tight = guarded_request(&h, &schema_pin, Some(action(8_000, vec![tightened])));
    let batch = decide(&h, tight).await.unwrap();
    assert!(matches!(
        batch.records.as_slice()[0].outcome,
        StatisticalOutcome::Abstained { .. }
    ));
    let loosened = RequestedBound {
        rule_id: "max-position".to_string(),
        bound: 50_000,
    };
    let loose = guarded_request(&h, &schema_pin, Some(action(12_000, vec![loosened])));
    let batch = decide(&h, loose).await.unwrap();
    let StatisticalOutcome::Abstained { reasons } = &batch.records.as_slice()[0].outcome else {
        panic!("a loosening request bound is ignored")
    };
    assert_eq!(
        reasons.as_slice(),
        [AbstainReason::LimitBreach {
            rule_id: "max-position".to_string()
        }]
    );
}

#[tokio::test]
async fn action_within_limits_is_not_refused() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);

    let within = guarded_request(&h, &schema_pin, Some(action(9_000, Vec::new())));
    let batch = decide(&h, within).await.unwrap();
    if let StatisticalOutcome::Abstained { reasons } = &batch.records.as_slice()[0].outcome {
        assert!(
            !reasons
                .iter()
                .any(|r| matches!(r, AbstainReason::LimitBreach { .. })),
            "an action inside its limits is never a limit_breach"
        );
    }
}

async fn dispatch(h: &Harness, method: crate::protocol::Method) -> crate::protocol::Response {
    let request = crate::protocol::Request {
        id: 21,
        graph: "__commons__".into(),
        auth_token: String::new(),
        agent_id: Some("decider".into()),
        method,
    };
    Box::pin(crate::server::dispatch::dispatch_verified_request(
        &h.state,
        request,
        verified(),
    ))
    .await
}

fn neighbors(response: crate::protocol::Response) -> Vec<String> {
    assert!(response.error.is_none(), "{response:?}");
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("expected a raw neighbor list")
    };
    rmp_serde::from_slice(&bytes).unwrap()
}

// spec: EG-DECISION-ENGINE-R131.5
#[tokio::test]
async fn breach_observation_links_signal() {
    use crate::protocol::Method;
    use eg_types::decision::statistical::log::{DecisionLogCommitted, DecisionLogOp};

    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);
    let signal = rmp_serde::to_vec_named(&serde_json::json!({"node_type": "Signal"})).unwrap();
    let added = dispatch(
        &h,
        Method::AddNode {
            node_id: "urn:signal:order-1".into(),
            properties_msgpack: signal,
        },
    )
    .await;
    assert!(added.error.is_none(), "{added:?}");

    let breaching = guarded_request(&h, &schema_pin, Some(action(12_000, Vec::new())));
    let record = decide(&h, breaching).await.unwrap().records.as_slice()[0].clone();
    assert_eq!(
        record.inputs.guarded_action.as_ref().unwrap().signal_iri,
        "urn:signal:order-1",
        "the sealed record carries the action it was checked on"
    );

    let commit = Method::DecisionLog {
        op: Box::new(DecisionLogOp::Commit {
            record: Box::new(record.clone()),
            evaluator: None,
        }),
    };
    let committed: DecisionLogCommitted = decode(dispatch(&h, commit).await).unwrap();
    assert!(!committed.replayed);

    let observation = crate::server::dispatch::limit_breach_observation_id(
        &record.record_id,
        "max-position",
    );
    let linked = neighbors(
        dispatch(
            &h,
            Method::GetNeighbors {
                node_id: observation.clone(),
            },
        )
        .await,
    );
    assert_eq!(linked, vec!["urn:signal:order-1".to_string()]);

    let properties = dispatch(
        &h,
        Method::GetNodeProperties {
            node_id: observation,
        },
    )
    .await;
    let properties: serde_json::Value = decode(properties).unwrap();
    assert_eq!(properties["rule_id"], "max-position");
    assert_eq!(properties["observed"], 12_000);
    assert_eq!(properties["bound"], 10_000);
}
