//! DecideText parses to typed requests and never to a `wire::Op`; the
//! ordinary UQL parser refuses its clauses by name.

use std::collections::BTreeMap;

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::{CandidateSource, QuestionKind, QuestionSafety, TypedValue};
use eg_types::decision::DecisionPolicyRef;

use super::{parse, DecideTextErrorKind, DecideTextRequest};

fn params() -> BTreeMap<String, TypedValue> {
    BTreeMap::from([
        (
            "plan_capabilities".to_string(),
            TypedValue::IriList(
                BoundedVec::new(vec!["eg:capability/retrieval".to_string()]).expect("fits"),
            ),
        ),
        (
            "query".to_string(),
            TypedValue::Text("web search".to_string()),
        ),
    ])
}

const DECIDE: &str = r#"CANDIDATES AGENT LIBRARY KINDS [tool, skill] UNDER eg:capability
|> COVERS @plan_capabilities
|> VALIDATE POLICY "policy-a" AT "sha256:p"
|> DECIDE route QUESTION "route.tools" SAFETY ordinary FEATURES "schema-a" AT "sha256:s" HEAD "head-a" AT "sha256:h" MAX 4"#;

#[test]
fn a_decide_clause_parses_to_a_typed_decide_request() {
    let DecideTextRequest::Decide(request) = parse(DECIDE, "tenant-a", &params()).expect("parses")
    else {
        panic!("a decide request")
    };
    assert_eq!(request.tenant_id, "tenant-a");
    assert_eq!(request.question.kind, QuestionKind::Route);
    assert_eq!(request.question.safety, QuestionSafety::Ordinary);
    assert!(matches!(
        request.candidates,
        CandidateSource::AgentLibrary { .. }
    ));
    assert!(matches!(request.policy, DecisionPolicyRef::Pinned { .. }));
    assert_eq!(
        request.head.as_ref().map(|h| h.component_id.as_str()),
        Some("head-a")
    );
    assert_eq!(request.max_records, Some(4));
    let names: Vec<&str> = request.params.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        names,
        ["plan_capabilities", "query"],
        "params travel typed and sorted"
    );
}

#[test]
fn an_assemble_clause_parses_to_an_assembly_request() {
    let text = "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS @plan_capabilities |> ASSEMBLE MAX COMPONENTS 3";
    let DecideTextRequest::Assemble(request) = parse(text, "t", &params()).expect("parses") else {
        panic!("an assembly request")
    };
    assert_eq!(request.requirements.capabilities.len(), 1);
    assert_eq!(request.requirements.constraints.max_components, Some(3));
}

#[test]
fn a_graph_candidate_query_is_ordinary_uql_inside_braces() {
    let text = r#"CANDIDATES GRAPH "kg" QUERY { MATCH (:Doc) WHERE year > 2020 |> LIMIT 5 }
|> DECIDE rank QUESTION "q" FEATURES "s" AT "sha256:s""#;
    let DecideTextRequest::Decide(request) = parse(text, "t", &params()).expect("parses") else {
        panic!("a decide request")
    };
    let CandidateSource::Graph { graph, plan } = &request.candidates else {
        panic!("graph candidates")
    };
    assert_eq!(graph, "kg");
    assert_eq!(plan.ops.len(), 3);
}

#[test]
fn typed_errors_name_what_is_wrong() {
    let cases = [
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS @missing |> ASSEMBLE",
            DecideTextErrorKind::UnboundParameter,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS @query |> ASSEMBLE",
            DecideTextErrorKind::ParameterType,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS @plan_capabilities",
            DecideTextErrorKind::MissingDecision,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> ASSEMBLE |> COVERS @query",
            DecideTextErrorKind::MissingDecision,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [nonsense] |> ASSEMBLE",
            DecideTextErrorKind::Syntax,
        ),
        (
            r#"CANDIDATES GRAPH "g" QUERY { NOT UQL } |> ASSEMBLE"#,
            DecideTextErrorKind::CandidateQuery,
        ),
    ];
    for (text, kind) in cases {
        assert_eq!(
            parse(text, "t", &params()).expect_err(text).kind,
            kind,
            "{text}"
        );
    }
}

#[test]
fn the_ordinary_uql_parser_refuses_decision_clauses_by_name() {
    for clause in [
        "DECIDE route",
        "ASSEMBLE",
        "COVERS @x",
        "VALIDATE POLICY DEFAULT",
    ] {
        let error = crate::uql::parse(&format!("MATCH (:Doc) |> {clause}")).expect_err(clause);
        assert!(
            error.msg.starts_with("DECISION_CLAUSE_IN_UQL"),
            "{clause}: {}",
            error.msg
        );
    }
}
