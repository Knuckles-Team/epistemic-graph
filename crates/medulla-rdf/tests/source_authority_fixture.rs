//! Source authority acceptance (MEDULLA-SRC-AUTH-R004.5, R009.1, R009.2): authority
//! rows are graph data. The same question text selects a different source when
//! only the ranks in the graph change.
#![cfg(feature = "rdf")]

use std::sync::Arc;

use medulla_core::graph::{GraphSchemaSource, GraphSchemaSources, SchemaSourceOrigin};
use medulla_rdf::source_selection::{
    audit_from, authority_rows_from_turtle, fixture_from_schema_sources, select_sources,
    AuthorityOrigin, AuthorityRow, CoverageFixture, CoverageResult, SelectionQuery,
};

const TEMPLATE: &str = include_str!("fixtures/source_authority_incident.ttl");
const SHAPES: &str = include_str!("../../medulla-core/ontology/virtual_graph-v1.shapes.ttl");
const INCIDENT: &str = "http://knuckles.team/kg#Incident";
const SNOW_ROW: &str = "urn:eg:fixture:incident:incident-servicenow";
const PD_ROW: &str = "urn:eg:fixture:incident:incident-pagerduty";

fn document(servicenow: u8, pagerduty: u8) -> String {
    TEMPLATE
        .replace("RANK_SERVICENOW", &servicenow.to_string())
        .replace("RANK_PAGERDUTY", &pagerduty.to_string())
}

/// The question: the ontology class only, never a source or connector name.
fn question() -> (Vec<String>, SelectionQuery) {
    (vec![INCIDENT.to_string()], SelectionQuery::default())
}

fn incident_verdict(fixture: &CoverageFixture) -> CoverageResult {
    let (roots, _) = question();
    select_sources(fixture, &roots, 1)
        .into_iter()
        .find(|v| v.class() == INCIDENT)
        .expect("an Incident verdict")
}

fn attached(document: &str) -> GraphSchemaSources {
    let mut sources = GraphSchemaSources::default();
    let source = GraphSchemaSource::new(
        SchemaSourceOrigin::Operator,
        None,
        Some(Arc::from(document)),
        0,
    )
    .unwrap();
    sources
        .attach_dynamic("operator".to_string(), source)
        .expect("the operator source attaches");
    sources
}

// spec: MEDULLA-SRC-AUTH-R009.1
#[test]
fn the_incident_fixture_parses_passes_the_shapes_and_selects_rank_one() {
    let doc = document(1, 2);
    let report = medulla_shacl::validate_turtle(SHAPES, &doc).expect("validates");
    assert!(report.conforms, "{:?}", report.results);
    let rows = authority_rows_from_turtle(&doc, AuthorityOrigin::Operator).unwrap();
    assert_eq!(rows.len(), 2);
    let fixture = rows
        .into_iter()
        .fold(CoverageFixture::new(), CoverageFixture::with_row);
    match incident_verdict(&fixture) {
        CoverageResult::Covered {
            source, alternates, ..
        } => {
            assert_eq!(source, "servicenow-mcp");
            assert_eq!(alternates, ["urn:eg:fixture:incident:pd-incidents"]);
        }
        other => panic!("expected ServiceNow, got {other:?}"),
    }
}

// spec: MEDULLA-SRC-AUTH-R009.2
#[test]
fn swapping_two_ranks_changes_sources_used_with_equal_query_text() {
    let before_question = question();
    let before = fixture_from_schema_sources(&attached(&document(1, 2))).unwrap();
    let before_audit = audit_from(&[incident_verdict(&before)]);
    let after_question = question();
    let after = fixture_from_schema_sources(&attached(&document(2, 1))).unwrap();
    let after_audit = audit_from(&[incident_verdict(&after)]);
    assert_eq!(before_question, after_question, "the query text is equal");
    let used = |audit: &medulla_rdf::source_selection::AuthorityAudit| {
        audit
            .used
            .iter()
            .map(|e| e.source.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(used(&before_audit), ["servicenow-mcp"]);
    assert_eq!(used(&after_audit), ["pagerduty-mcp"]);
    assert!(!before_audit.used[0].premises.is_empty());
    assert!(!after_audit.used[0].premises.is_empty());
}

// spec: MEDULLA-SRC-AUTH-R004.5
#[test]
fn the_live_read_returns_the_same_verdict_as_the_fixture() {
    let live = fixture_from_schema_sources(&attached(&document(1, 2))).unwrap();
    let row = |id: &str, mapping: &str, source: &str, rank: u64| AuthorityRow {
        row: id.to_string(),
        mapping_name: format!("urn:eg:fixture:incident:{mapping}"),
        contract: Some("mcp".to_string()),
        ..AuthorityRow::new(INCIDENT, mapping, source, 0, rank).origin(AuthorityOrigin::Operator)
    };
    let hand = CoverageFixture::new()
        .with_row(row(SNOW_ROW, "snow-incidents", "servicenow-mcp", 1))
        .with_row(row(PD_ROW, "pd-incidents", "pagerduty-mcp", 2))
        .with_source_kind("servicenow-mcp", "mcp")
        .with_source_kind("pagerduty-mcp", "mcp");
    let live_verdict = incident_verdict(&live);
    let hand_verdict = incident_verdict(&hand);
    let strip_subclass = |v: CoverageResult| match v {
        CoverageResult::Covered {
            source,
            mapping_name,
            alternates,
            premises,
            mode,
            ..
        } => (source, mapping_name, alternates, premises, mode),
        other => panic!("expected Covered, got {other:?}"),
    };
    assert_eq!(strip_subclass(live_verdict), strip_subclass(hand_verdict));
    // A pack-origin read of the same text proposes only.
    let proposed = authority_rows_from_turtle(&document(1, 2), AuthorityOrigin::Pack).unwrap();
    let fixture = proposed
        .into_iter()
        .fold(CoverageFixture::new(), CoverageFixture::with_row);
    assert!(matches!(
        incident_verdict(&fixture),
        CoverageResult::Uncovered { .. }
    ));
}
