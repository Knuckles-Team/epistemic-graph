//! Orchestration competency questions (MEDULLA-CQ-ORCH Q1-Q7): each question has its
//! ontology terms, shape, source-authority mapping, fixture and acceptance test.
//!
//! Every fixture is a disposable Turtle graph under
//! `crates/medulla-core/tests/fixtures/orchestration_competency/q{n}` with its query
//! and expected rows. A `dc:source` triple stands in for the provenance the
//! virtual mapping attaches to fetched records. The source-authority mapping is
//! the fixture-backed selection model in `medulla_rdf::source_selection`.
#![cfg(all(feature = "owl", feature = "sparql"))]

mod common;

use common::parse_all;
use medulla_core::graph::{GraphCore, GraphView};
use medulla_rdf::mapping::{load_triples, parse_turtle, IriStore};
use medulla_rdf::owl::Reasoner;
use medulla_rdf::source_selection::{select_sources, CoverageFixture, CoverageResult, Premise};
use medulla_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};
use medulla_shacl::validate_turtle;

const SCHEMA: &[&str] = &[
    include_str!("../../medulla-core/ontology/core-foundation-v1.ttl"),
    include_str!("../../medulla-core/ontology/orchestration-v1.ttl"),
    include_str!("../../medulla-core/ontology/capability-v1.ttl"),
    include_str!("../../medulla-core/ontology/a2a-v1.ttl"),
    include_str!("../../medulla-core/ontology/sdd-v1.ttl"),
    include_str!("../../medulla-core/ontology/swarm_topology-v1.ttl"),
];
const SHAPES: &str = include_str!("../../medulla-core/ontology/orchestration-v1.shapes.ttl");
const SWARM_SHAPES: &str = include_str!("../../medulla-core/ontology/swarm_topology-v1.shapes.ttl");
const PREFIXES: &str = "PREFIX : <http://knuckles.team/kg#> \
    PREFIX swarm: <http://knuckles.team/kg/swarm#> \
    PREFIX owl: <http://www.w3.org/2002/07/owl#> \
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> ";
const TURTLE_PREFIXES: &str = "@prefix : <http://knuckles.team/kg#> . \
    @prefix swarm: <http://knuckles.team/kg/swarm#> . \
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . \n";

struct Question {
    fixture: &'static str,
    query: &'static str,
    expected: &'static str,
    /// The ontology class the question's concept maps to, and its authority.
    concept: &'static str,
    source: &'static str,
}

macro_rules! question {
    ($n:literal, $concept:literal, $source:literal) => {
        Question {
            fixture: include_str!(concat!(
                "../../medulla-core/tests/fixtures/orchestration_competency/q",
                $n,
                "/fixture.ttl"
            )),
            query: include_str!(concat!(
                "../../medulla-core/tests/fixtures/orchestration_competency/q",
                $n,
                "/query.rq"
            )),
            expected: include_str!(concat!(
                "../../medulla-core/tests/fixtures/orchestration_competency/q",
                $n,
                "/expected.tsv"
            )),
            concept: $concept,
            source: $source,
        }
    };
}

fn q(n: usize) -> Question {
    match n {
        1 => question!("1", "TaskStep", "task-decomposition"),
        2 => question!("2", "Slot", "topology-templates"),
        3 => question!("3", "AgentTemplate", "agent-library"),
        4 => question!("4", "BestPractice", "guardrail-rules"),
        5 => question!("5", "ModelProfile", "assembly"),
        6 => question!("6", "TaskStep", "workflow-store"),
        7 => question!("7", "TopologyTemplate", "topology-templates"),
        _ => unreachable!(),
    }
}

fn view_of(fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(SCHEMA)).unwrap();
    load_triples(&core, &mut iris, "fixture", parse_turtle(fixture).unwrap()).unwrap();
    core.analysis_snapshot()
}

fn outcome(view: &GraphView, query: &str) -> QueryOutcome {
    execute(
        &Dataset::new(view, Vec::new()),
        &format!("{PREFIXES}{query}"),
        &Projection::raw(),
        None,
    )
    .unwrap()
}

fn ask(view: &GraphView, pattern: &str) -> bool {
    match outcome(view, &format!("ASK {{ {pattern} }}")) {
        QueryOutcome::Boolean(value) => value,
        other => panic!("expected a boolean, got {other:?}"),
    }
}

/// The answer rows as sorted tab-separated local names; `-` is an unbound cell.
fn answer(question: &Question) -> Vec<String> {
    let view = view_of(question.fixture);
    let QueryOutcome::Solutions(table) = outcome(&view, question.query) else {
        panic!("expected solutions");
    };
    let mut rows: Vec<String> = table
        .to_rows()
        .1
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| match cell {
                    None => "-".to_string(),
                    Some(value) => value
                        .trim_matches(|c| c == '<' || c == '>' || c == '"')
                        .rsplit(['#', '/', ':'])
                        .next()
                        .unwrap()
                        .to_string(),
                })
                .collect::<Vec<_>>()
                .join("\t")
        })
        .collect();
    rows.sort();
    rows
}

fn expected(question: &Question) -> Vec<String> {
    let mut rows: Vec<String> = question.expected.lines().map(str::to_string).collect();
    rows.sort();
    rows
}

fn declared(view: &GraphView, term: &str, kind: &str) {
    assert!(ask(view, &format!("{term} a {kind}")), "{term} a {kind}");
}

fn consistent(extra: &str) {
    let mut triples = parse_all(SCHEMA);
    triples.extend(parse_turtle(extra).unwrap());
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
}

fn messages(shapes: &str, shape: &str, data: &str) -> Vec<String> {
    validate_turtle(shapes, data)
        .unwrap()
        .results
        .into_iter()
        .filter(|result| {
            result.source_shape.contains(shape)
                || (shapes == SHAPES && result.source_shape.starts_with("_:"))
        })
        .map(|result| result.message.unwrap_or(result.constraint_component))
        .collect()
}

fn planted(snippet: &str) -> String {
    format!("{TURTLE_PREFIXES}{snippet}")
}

/// The source-authority mapping of one question: the declared source covers its
/// concept class; a class with no mapping is the typed abstention.
fn authority(question: &Question) -> CoverageFixture {
    CoverageFixture::new()
        .with_relation(question.concept, "relatedTo", "Unmapped")
        .with_authority(
            question.concept,
            &format!("{}-mapping", question.source),
            question.source,
            1,
            1,
        )
}

fn assert_authority_selects_declared_source(question: &Question) {
    let roots = [question.concept.to_string()];
    let results = select_sources(&authority(question), &roots, 1);
    let concept = results
        .iter()
        .find(|result| result.class() == question.concept)
        .unwrap();
    match concept {
        CoverageResult::Covered {
            mapping_name,
            premises,
            ..
        } => {
            assert_eq!(mapping_name, &format!("{}-mapping", question.source));
            assert!(premises.iter().any(|premise| matches!(
                premise,
                Premise::Authority { source, .. } if source == question.source
            )));
        }
        other => panic!("expected the declared source to cover the concept, got {other:?}"),
    }
    // A class no connected source maps is the typed abstention, never a guess.
    assert!(matches!(
        results.iter().find(|r| r.class() == "Unmapped").unwrap(),
        CoverageResult::Uncovered { .. }
    ));
    let unconnected = select_sources(
        &CoverageFixture::new().with_relation(question.concept, "relatedTo", "Unmapped"),
        &roots,
        1,
    );
    assert!(matches!(
        unconnected
            .iter()
            .find(|r| r.class() == question.concept)
            .unwrap(),
        CoverageResult::Uncovered { .. }
    ));
    let disconnected = select_sources(
        &CoverageFixture::new().with_relation("Elsewhere", "relatedTo", "Unmapped"),
        &roots,
        1,
    );
    assert!(matches!(
        disconnected
            .iter()
            .find(|r| r.class() == "Elsewhere")
            .unwrap(),
        CoverageResult::Disconnected { .. }
    ));
}

fn assert_answers_with_provenance(n: usize) {
    let question = q(n);
    let rows = answer(&question);
    assert!(!rows.is_empty(), "Q{n} answered nothing");
    assert!(
        rows.iter()
            .all(|row| row.rsplit('\t').next() == Some(question.source)),
        "every Q{n} row carries its source: {rows:?}"
    );
    assert_authority_selects_declared_source(&question);
}

fn q_fixture_rows(n: usize) {
    let question = q(n);
    assert_eq!(answer(&question), expected(&question));
    assert!(!expected(&question).is_empty());
}

// spec: MEDULLA-CQ-ORCH-R001.1
#[test]
fn q1_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, ":TaskStep", "owl:Class");
    declared(&view, ":hasStep", "owl:ObjectProperty");
    assert!(ask(&view, ":precedes rdfs:domain ?d ; rdfs:range ?r"));
    consistent(&format!(
        "{TURTLE_PREFIXES}:a a :TaskStep ; :precedes :b . :b a :TaskStep ."
    ));
}

// spec: MEDULLA-CQ-ORCH-R001.2
#[test]
fn q1_shape_accepts_fixture_and_rejects_a_cycle() {
    let fixture = q(1).fixture;
    assert!(messages(SHAPES, "TaskStepShape", fixture).is_empty());
    let cycle = planted(
        ":t :hasStep :a, :b . :a a :TaskStep ; :precedes :b . :b a :TaskStep ; :precedes :a .",
    );
    assert!(!messages(SHAPES, "TaskStepShape", &cycle).is_empty());
}

// spec: MEDULLA-CQ-ORCH-R001.3
#[test]
fn q1_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(1));
}

// spec: MEDULLA-CQ-ORCH-R001.4
#[test]
fn q1_fixture_returns_expected_rows() {
    q_fixture_rows(1);
}

// spec: MEDULLA-CQ-ORCH-R001.5
#[test]
fn q1_answers_with_provenance() {
    assert_answers_with_provenance(1);
}

// spec: MEDULLA-CQ-ORCH-R002.1
#[test]
fn q2_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, ":AgentKind", "owl:Class");
    declared(&view, ":hasAgentKind", "owl:ObjectProperty");
    declared(&view, "swarm:requiresAgentKind", "owl:ObjectProperty");
    assert!(ask(&view, ":agentKindVerifier a :AgentKind"));
    consistent(&format!(
        "{TURTLE_PREFIXES}:s a swarm:Slot ; swarm:requiresAgentKind :agentKindVerifier ."
    ));
}

// spec: MEDULLA-CQ-ORCH-R002.2
#[test]
fn q2_shape_accepts_fixture_and_rejects_bad_width() {
    let fixture = q(2).fixture;
    let report = validate_turtle(SWARM_SHAPES, fixture).unwrap();
    assert!(report.conforms, "{:?}", report.results);
    let inverted = fixture.replace(
        "swarm:minWidth 1 ; swarm:maxWidth 2",
        "swarm:minWidth 3 ; swarm:maxWidth 2",
    );
    assert_ne!(inverted, fixture);
    assert!(!validate_turtle(SWARM_SHAPES, &inverted).unwrap().conforms);
    let unbounded = fixture.replace(
        "swarm:minWidth 1 ; swarm:maxWidth 2",
        "swarm:minWidth 1 ; swarm:maxWidth 99",
    );
    assert!(!validate_turtle(SWARM_SHAPES, &unbounded).unwrap().conforms);
}

// spec: MEDULLA-CQ-ORCH-R002.3
#[test]
fn q2_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(2));
}

// spec: MEDULLA-CQ-ORCH-R002.4
#[test]
fn q2_fixture_returns_expected_rows() {
    q_fixture_rows(2);
}

// spec: MEDULLA-CQ-ORCH-R002.5
#[test]
fn q2_answers_with_provenance() {
    assert_answers_with_provenance(2);
}

// spec: MEDULLA-CQ-ORCH-R003.1
#[test]
fn q3_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, ":AgentTemplate", "owl:Class");
    declared(&view, ":AgentLibrary", "owl:Class");
    declared(&view, ":memberOfLibrary", "owl:ObjectProperty");
    consistent(q(3).fixture);
}

// spec: MEDULLA-CQ-ORCH-R003.2
#[test]
fn q3_shape_accepts_fixture_and_rejects_member_without_capability() {
    assert!(messages(SHAPES, "LibraryMemberShape", q(3).fixture).is_empty());
    let useless = planted(":useless a :Agent ; :memberOfLibrary :libXYZ .");
    assert!(!messages(SHAPES, "LibraryMemberShape", &useless).is_empty());
}

// spec: MEDULLA-CQ-ORCH-R003.3
#[test]
fn q3_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(3));
}

// spec: MEDULLA-CQ-ORCH-R003.4
#[test]
fn q3_fixture_returns_expected_rows() {
    q_fixture_rows(3);
}

// spec: MEDULLA-CQ-ORCH-R003.5
#[test]
fn q3_answers_with_provenance() {
    assert_answers_with_provenance(3);
}

// spec: MEDULLA-CQ-ORCH-R004.1
#[test]
fn q4_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, ":BestPractice", "owl:Class");
    declared(&view, ":appliesToTaskClass", "owl:ObjectProperty");
    assert!(ask(
        &view,
        ":BestPractice rdfs:subClassOf :DevelopmentStandard"
    ));
    consistent(q(4).fixture);
}

// spec: MEDULLA-CQ-ORCH-R004.2
#[test]
fn q4_shape_accepts_fixture_and_rejects_rule_without_class() {
    assert!(messages(SHAPES, "RuleScopeShape", q(4).fixture).is_empty());
    let unscoped = planted(":rule a :BestPractice .");
    assert!(!messages(SHAPES, "RuleScopeShape", &unscoped).is_empty());
}

// spec: MEDULLA-CQ-ORCH-R004.3
#[test]
fn q4_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(4));
}

// spec: MEDULLA-CQ-ORCH-R004.4
#[test]
fn q4_fixture_returns_expected_rows() {
    q_fixture_rows(4);
}

// spec: MEDULLA-CQ-ORCH-R004.5
#[test]
fn q4_answers_with_provenance() {
    assert_answers_with_provenance(4);
}

// spec: MEDULLA-CQ-ORCH-R005.1
#[test]
fn q5_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, ":ModelProfile", "owl:Class");
    declared(&view, ":needsSkill", "owl:ObjectProperty");
    declared(&view, ":stepAssignedAgent", "owl:ObjectProperty");
    consistent(q(5).fixture);
}

// spec: MEDULLA-CQ-ORCH-R005.2
#[test]
fn q5_shape_accepts_fixture_and_rejects_agent_without_model() {
    assert!(messages(SHAPES, "StepBindingShape", q(5).fixture).is_empty());
    let bare = planted(":s a :TaskStep ; :stepAssignedAgent :x . :x a :Agent ; :hasPrompt :p .");
    assert!(!messages(SHAPES, "StepBindingShape", &bare).is_empty());
}

// spec: MEDULLA-CQ-ORCH-R005.3
#[test]
fn q5_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(5));
}

// spec: MEDULLA-CQ-ORCH-R005.4
#[test]
fn q5_fixture_returns_expected_rows() {
    q_fixture_rows(5);
}

// spec: MEDULLA-CQ-ORCH-R005.5
#[test]
fn q5_answers_with_provenance() {
    assert_answers_with_provenance(5);
}

// spec: MEDULLA-CQ-ORCH-R006.1
#[test]
fn q6_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, ":handsOffTo", "owl:ObjectProperty");
    assert!(ask(&view, ":handsOffTo rdfs:subPropertyOf :precedes"));
    consistent(q(6).fixture);
}

// spec: MEDULLA-CQ-ORCH-R006.2
#[test]
fn q6_shape_accepts_fixture_and_rejects_hand_off_to_a_non_step() {
    assert!(messages(SHAPES, "HandOffShape", q(6).fixture).is_empty());
    let target = planted(":a a :TaskStep ; :handsOffTo :b .");
    assert!(messages(SHAPES, "HandOffShape", &target)
        .iter()
        .any(|m| m.contains("target")));
    let source = planted(":a :handsOffTo :b . :b a :TaskStep .");
    assert!(messages(SHAPES, "HandOffShape", &source)
        .iter()
        .any(|m| m.contains("source")));
}

// spec: MEDULLA-CQ-ORCH-R006.3
#[test]
fn q6_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(6));
}

// spec: MEDULLA-CQ-ORCH-R006.4
#[test]
fn q6_fixture_returns_expected_rows() {
    q_fixture_rows(6);
}

// spec: MEDULLA-CQ-ORCH-R006.5
#[test]
fn q6_answers_with_provenance() {
    assert_answers_with_provenance(6);
}

// spec: MEDULLA-CQ-ORCH-R007.1
#[test]
fn q7_ontology_terms_resolve() {
    let view = view_of("");
    declared(&view, "swarm:feeds", "owl:ObjectProperty");
    assert!(ask(
        &view,
        "swarm:feeds rdfs:domain swarm:Slot ; rdfs:range swarm:Slot"
    ));
    consistent(q(7).fixture);
}

// spec: MEDULLA-CQ-ORCH-R007.2
#[test]
fn q7_shape_accepts_fixture_and_rejects_a_cycle_or_a_second_root() {
    let fixture = q(7).fixture;
    for shape in ["FeedsAcyclicShape", "FeedsSingleRootShape"] {
        assert!(messages(SWARM_SHAPES, shape, fixture).is_empty(), "{shape}");
    }
    let cyclic = fixture.replace(
        ":dLeaf a swarm:Slot .",
        ":dLeaf a swarm:Slot ; swarm:feeds :dRoot .",
    );
    assert_ne!(cyclic, fixture);
    assert!(!messages(SWARM_SHAPES, "FeedsAcyclicShape", &cyclic).is_empty());
    let two_roots = fixture.replace(
        ":dMid a swarm:Slot ; swarm:feeds :dLeaf .",
        ":dMid a swarm:Slot .",
    );
    assert_ne!(two_roots, fixture);
    assert!(!messages(SWARM_SHAPES, "FeedsSingleRootShape", &two_roots).is_empty());
}

// spec: MEDULLA-CQ-ORCH-R007.3
#[test]
fn q7_source_authority_selects_declared_source() {
    assert_authority_selects_declared_source(&q(7));
}

// spec: MEDULLA-CQ-ORCH-R007.4
#[test]
fn q7_fixture_returns_expected_rows() {
    q_fixture_rows(7);
}

// spec: MEDULLA-CQ-ORCH-R007.5
#[test]
fn q7_answers_with_provenance() {
    assert_answers_with_provenance(7);
}
