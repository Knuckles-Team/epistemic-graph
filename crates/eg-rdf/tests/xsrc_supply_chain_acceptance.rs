//! Recorded-response acceptance for the supply-chain impact question
//! (EG-CQ-XSRC-R007.1): the class-only question runs over recorded
//! erpnext-mcp, dockerhub-mcp, gitlab-mcp, container-manager-mcp and
//! uptime-mcp responses (no network, no live credentials), the sources are
//! planned from the seeded authority rows, and the assembled report is
//! non-empty, provenance-carrying, and cites only sources that appear in an
//! authority row.
#![cfg(feature = "sparql")]

mod common;

use std::collections::BTreeSet;

use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_rdf::source_selection::CoverageFixture;
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};
use eg_rdf::supply_chain_impact::supply_chain_impact;
use eg_types::supply_chain_report::{ImpactKind, ImpactRow};

use common::parse_all;

const QUESTION: &str = include_str!("fixtures/xsrc_supply_chain_impact.sparql");
const RECORDED: &str = include_str!("fixtures/recorded_supply_chain_responses.ttl");

/// Load the assigned vocabulary and the recorded responses into one graph and
/// return the analysis view the SPARQL evaluator scans.
fn graph() -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(
        &core,
        &mut iris,
        "schema",
        parse_all(&[
            include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
            include_str!("../../eg-core/ontology/sdd-v1.ttl"),
            include_str!("../../eg-core/ontology/trm-v1.ttl"),
            include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
            include_str!("../../eg-core/ontology/supply_chain-v1.ttl"),
        ]),
    )
    .unwrap();
    load_triples(
        &core,
        &mut iris,
        "recorded",
        parse_turtle(RECORDED).unwrap(),
    )
    .unwrap();
    core.analysis_snapshot()
}

/// Execute a SELECT and return every cell as its lexical form; an unbound
/// OPTIONAL variable is `None`.
fn select(view: &GraphView, query: &str) -> Vec<Vec<Option<String>>> {
    let outcome = execute(
        &Dataset::new(view, Vec::new()),
        query,
        &Projection::raw(),
        None,
    )
    .expect("the question executes over the recorded responses");
    let table = match outcome {
        QueryOutcome::Solutions(table) => table,
        other => panic!("expected a SELECT solution table, got {other:?}"),
    };
    let (_, rows) = table.to_rows();
    rows
}

/// The local name of a bracketed node-id cell (`<urn:…/host-1>` → `host-1`).
fn local(cell: &str) -> String {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
        .to_string()
}

// spec: EG-CQ-XSRC-R007.1
#[test]
fn the_recorded_responses_answer_the_impact_question_with_provenance() {
    let view = graph();

    // The class-only question answers from the recorded data.
    let rows = select(&view, QUESTION);
    assert!(
        !rows.is_empty(),
        "the recorded responses must produce at least one impact row"
    );

    // Each row's component version, and the source that recorded it
    // (provenance the connector's virtual mapping attaches).
    let versions = select(
        &view,
        r#"
        PREFIX : <http://knuckles.team/kg#>
        PREFIX dc: <http://purl.org/dc/terms/>
        SELECT ?component ?source ?identifier ?date WHERE {
          ?version :supplyChainVersionOf ?component ;
                   dc:source ?source ;
                   dc:identifier ?identifier ;
                   dc:date ?date .
        }
        "#,
    );
    let mut version_lookup: std::collections::BTreeMap<String, (String, String, String)> =
        std::collections::BTreeMap::new();
    for row in versions {
        version_lookup.insert(
            local(row[0].as_deref().expect("the component cell binds")),
            (
                row[1].clone().expect("the source cell binds"),
                row[2].clone().expect("the identifier cell binds"),
                row[3].clone().expect("the as-of cell binds"),
            ),
        );
    }

    // Build the planned rows: the question's solution mapped to the recorded
    // source of each item's facts.
    let mut impact_rows = Vec::new();
    let mut item_provenance = Vec::new();
    for row in &rows {
        let service = row[0].as_deref().expect("the service cell binds");
        let component = row[1].as_deref().expect("the component cell binds");
        let supplier = row[2].as_deref().expect("the supplier cell binds");
        let vulnerability = row[3].clone();
        let Some((source, identifier, date)) = version_lookup.get(&local(component)) else {
            panic!("the component {component} has no recorded version provenance");
        };
        impact_rows.push(ImpactRow {
            service: service.to_string(),
            supplier: supplier.to_string(),
            component_version: component.to_string(),
            vulnerability,
            impact_kind: ImpactKind::RuntimeChain,
            source: source.clone(),
        });
        item_provenance.push((source.clone(), identifier.clone(), date.clone()));
    }
    assert!(!impact_rows.is_empty());

    // The sources are planned from the authority rows, not named in the text:
    // only the classes the question asks about carry rows in the plan.
    let mut fixture = CoverageFixture::new();
    for (class, mapping, source, rank) in [
        ("Service", "uptime-service", "uptime-mcp", 1),
        ("SoftwareComponent", "github-components", "github-mcp", 1),
        (
            "SoftwareComponent",
            "dockerhub-components",
            "dockerhub-mcp",
            2,
        ),
        ("Supplier", "erpnext-suppliers", "erpnext-mcp", 1),
        ("Supplier", "leanix-suppliers", "leanix-mcp", 2),
        ("Vulnerability", "gitlab-vulnerabilities", "gitlab-mcp", 1),
        ("Vulnerability", "github-vulnerabilities", "github-mcp", 2),
    ] {
        let class_iri = if class == "Service" {
            "http://knuckles.team/kg/infrastructure#Service".to_string()
        } else {
            format!("http://knuckles.team/kg#{class}")
        };
        fixture = fixture.with_authority(&class_iri, mapping, source, 1, rank);
    }
    let roots = vec![
        "http://knuckles.team/kg/infrastructure#Service".to_string(),
        "http://knuckles.team/kg#SoftwareComponent".to_string(),
        "http://knuckles.team/kg#Supplier".to_string(),
        "http://knuckles.team/kg#Vulnerability".to_string(),
    ];

    let report = supply_chain_impact(&fixture, &roots, 1, &impact_rows)
        .expect("the report assembles from the planned rows");

    // At least one ServiceImpact with a supplier, a component version and a
    // source per item.
    let Some(impact) = report.service_impacts.iter().find(|impact| {
        impact.items.iter().all(|item| {
            !item.supplier.is_empty()
                && !item.component_version.is_empty()
                && !item.source.is_empty()
        })
    }) else {
        panic!("expected a ServiceImpact with supplier, version and source per item");
    };
    assert!(!impact.items.is_empty());
    assert!(
        report
            .sources_used
            .iter()
            .any(|source| !source.source.is_empty() && !source.authority_premise.is_empty()),
        "sources used must carry their authority premise"
    );

    // Every source named in the report appears in an authority row.
    let authority_sources: BTreeSet<&str> = [
        "uptime-mcp",
        "github-mcp",
        "dockerhub-mcp",
        "erpnext-mcp",
        "leanix-mcp",
        "gitlab-mcp",
    ]
    .into_iter()
    .collect();
    for source in &report.sources_used {
        assert!(
            authority_sources.contains(source.source.as_str()),
            "the report cites {source:?}, which no authority row names"
        );
    }
    for item in &impact.items {
        assert!(
            authority_sources.contains(item.source.as_str()),
            "the item cites {item:?}, which no authority row names"
        );
    }

    // Provenance: each item's source is the recorded connector that holds the
    // version record, with a record id and a frozen as-of.
    for (source, identifier, date) in &item_provenance {
        assert!(
            identifier.starts_with("dockerhub:rec:") || identifier.contains(":rec:"),
            "the item provenance names a record id, got {identifier:?}"
        );
        assert!(
            date.ends_with("Z"),
            "the item provenance carries an as-of, got {date:?}"
        );
        assert!(
            !source.is_empty(),
            "the item provenance names the recording source"
        );
    }
}
