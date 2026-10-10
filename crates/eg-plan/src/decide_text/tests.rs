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

const DECIDE: &str = r#"CANDIDATES AGENT LIBRARY KINDS [tool, skill] UNDER <eg:capability>
|> COVERS $plan_capabilities
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
    let CandidateSource::AgentLibrary { scope } = &request.candidates else {
        unreachable!()
    };
    assert_eq!(scope.classification_under.as_deref(), Some("eg:capability"));
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
    let text = "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS $plan_capabilities |> ASSEMBLE MAX COMPONENTS 3";
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
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS $missing |> ASSEMBLE",
            DecideTextErrorKind::UnboundParameter,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS $query |> ASSEMBLE",
            DecideTextErrorKind::ParameterType,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS $plan_capabilities",
            DecideTextErrorKind::MissingDecision,
        ),
        (
            "CANDIDATES AGENT LIBRARY KINDS [tool] |> ASSEMBLE |> COVERS $query",
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

// spec: EG-DECISION-ENGINE-R071
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

/// EH-452: one lexical family with UQL — `$name` parameters (the old `@name` is refused
/// with the fix), quote-aware candidate blocks, and structured errors whose candidate
/// query diagnostic points into the DecideText source.
// spec: EG-DECISION-ENGINE-R104
#[test]
fn decide_text_shares_the_uql_lexical_family() {
    let at_sign = parse(
        "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS @plan_capabilities |> ASSEMBLE",
        "t",
        &params(),
    )
    .expect_err("`@name` is not a parameter");
    assert_eq!(at_sign.kind, DecideTextErrorKind::Syntax);
    assert!(at_sign.help.as_deref().unwrap_or("").contains("$name"));

    let quoted = r#"CANDIDATES GRAPH "kg" QUERY { MATCH (:Doc) WHERE note = '}' |> LIMIT 2 }
|> DECIDE rank QUESTION "q" FEATURES "s" AT "sha256:s""#;
    let DecideTextRequest::Decide(request) = parse(quoted, "t", &params()).expect("parses") else {
        panic!("a decide request")
    };
    let CandidateSource::Graph { plan, .. } = &request.candidates else {
        panic!("graph candidates")
    };
    assert_eq!(
        plan.ops.len(),
        3,
        "the `}}` inside the string does not close the block"
    );

    let src = r#"CANDIDATES GRAPH "g" QUERY { MATCH (:Doc) |> LIMIT many } |> ASSEMBLE"#;
    let error = parse(src, "t", &params()).expect_err("an invalid candidate query");
    assert_eq!(error.kind, DecideTextErrorKind::CandidateQuery);
    let cause = error.cause.as_ref().expect("the UQL diagnostic is kept");
    assert_eq!(
        &src[cause.at..cause.end],
        "many",
        "rebased onto the DecideText source"
    );
    let rendered = error.render(src);
    assert!(
        rendered.starts_with("DECIDE_TEXT_CANDIDATE_QUERY: UQL_"),
        "{rendered}"
    );

    let unknown = parse(
        "CANDIDATES AGENT LIBRARY KINDS [tool] |> SOLVE",
        "t",
        &params(),
    )
    .expect_err("an unknown clause");
    assert_eq!(
        unknown.expected,
        ["`COVERS`", "`VALIDATE`", "`DECIDE`", "`ASSEMBLE`"]
    );
    assert!(unknown
        .render("CANDIDATES AGENT LIBRARY KINDS [tool] |> SOLVE")
        .contains('^'));
}

#[test]
fn the_grammar_table_matches_the_parser_and_its_examples_parse() {
    let table: Vec<&str> = super::parser::Parser::clause_table()
        .iter()
        .map(|(kw, _)| *kw)
        .collect();
    assert_eq!(table, super::grammar::clause_keywords());
    let mut bound = params();
    bound.insert(
        "capabilities".to_string(),
        TypedValue::IriList(BoundedVec::new(vec!["eg:cap".to_string()]).expect("fits")),
    );
    for prod in super::grammar::PRODUCTIONS
        .iter()
        .filter(|p| !p.example.is_empty())
    {
        if let Err(e) = parse(prod.example, "t", &bound) {
            panic!("{} example: {}", prod.name, e.render(prod.example));
        }
    }
    assert!(super::grammar::ebnf().contains("decide_text"));
}
