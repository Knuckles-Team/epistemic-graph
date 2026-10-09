//! EG-DECISION-ENGINE-R045.2: a served `ToolSubset` question selects the
//! smallest covering tool subset within the caller's context budget.

use super::*;
use eg_types::decision::statistical::declared::{DeclaredNumber, DeclaredOption};

fn tool(id: &str, capabilities: &[&str], token_cost: i64) -> DeclaredOption {
    DeclaredOption {
        option_id: id.to_string(),
        classification: BoundedVec::new(capabilities.iter().map(|s| s.to_string()).collect())
            .unwrap(),
        numbers: BoundedVec::new(vec![DeclaredNumber {
            key: "token_cost".to_string(),
            q32: token_cost,
        }])
        .unwrap(),
        texts: BoundedVec::default(),
    }
}

fn tool_subset_request(
    schema: &ComponentDependency,
    required: &[&str],
    budget_tokens: i64,
    tools: Vec<DeclaredOption>,
) -> DecideRequest {
    DecideRequest {
        tenant_id: TENANT.to_string(),
        question: StatisticalQuestion {
            question_id: "mux.tool-subset".to_string(),
            kind: QuestionKind::ToolSubset,
            safety: QuestionSafety::Ordinary,
        },
        candidates: CandidateSource::Declared {
            options: BoundedVec::new(tools).unwrap(),
        },
        feature_schema: schema.clone(),
        head: None,
        policy: DecisionPolicyRef::Default,
        params: BoundedVec::new(vec![
            TypedParam {
                name: "required_capabilities".to_string(),
                value: TypedValue::IriList(
                    BoundedVec::new(required.iter().map(|s| s.to_string()).collect()).unwrap(),
                ),
            },
            TypedParam {
                name: "context_budget_tokens".to_string(),
                value: TypedValue::Int(budget_tokens),
            },
        ])
        .unwrap(),
        max_records: None,
        belief_as_of: BoundedVec::default(),
    }
}

#[tokio::test]
async fn tool_subset_question_selects_the_minimal_covering_tools_within_budget() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);

    let request = tool_subset_request(
        &schema_pin,
        &["search", "fetch", "rank"],
        100,
        vec![
            tool("broad-tool", &["search", "fetch", "rank"], 30),
            tool("fetch-only", &["fetch"], 50),
            tool("search-only", &["search"], 50),
        ],
    );
    let batch = decide(&h, request).await.unwrap();
    let record = &batch.records.as_slice()[0];

    assert_eq!(record.question.kind, QuestionKind::ToolSubset);
    let StatisticalOutcome::Advisory { scores, calibrated } = &record.outcome else {
        panic!(
            "expected an Advisory outcome carrying the selected subset: {:?}",
            record.outcome
        )
    };
    assert!(!calibrated, "a selection is not a calibrated probability");
    assert_eq!(
        scores
            .iter()
            .map(|s| s.option_id.as_str())
            .collect::<Vec<_>>(),
        vec!["broad-tool"],
        "the single cheap tool that covers every capability is the minimal subset"
    );

    // resolution_kind/evidence_class are generic, computed the same way as
    // every other question kind (stat_decide::resolution/evidence_class).
    assert_eq!(
        record.resolution_kind,
        eg_types::decision::ResolutionKind::Statistical
    );
}

#[tokio::test]
async fn tool_subset_question_refuses_when_the_budget_cannot_cover_every_capability() {
    let h = Harness::new().await;
    let (schema_pin, _digest) = publish_route_schema(&h);

    let request = tool_subset_request(
        &schema_pin,
        &["search", "fetch"],
        10,
        vec![tool("broad-tool", &["search", "fetch"], 50)],
    );
    let refused = decide(&h, request).await.unwrap_err();
    assert!(
        refused.starts_with("PARAMETER_INVALID"),
        "expected a parameter-invalid refusal, got {refused}"
    );
}
