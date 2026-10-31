//! Connector competency questions (MEDULLA-CQ-CONN), chunk-1 lane: fixture
//! SPARQL answers for the chunk's question rows that are fully expressible
//! on the vocabulary on main (the amendment pattern recorded in
//! `specs/connector-competency/plan.md`).
//!
//! The rest of chunk 1 (R001-R017) is blocked on unlanded producer rows
//! R068.2-R082.1 or belongs to the 23 out-of-scope MISSING-vocabulary
//! connectors; this file delivers the two rows whose questions the
//! vocabulary on main already answers, following the fixture pattern
//! established in `connector_competency.rs`: a disposable Turtle fixture
//! (no live credentials, no network), the assigned vocabulary module, the
//! question executed through the native SPARQL surface, and a non-empty
//! answer whose rows carry provenance (source, record id, as-of). The
//! `dc:` provenance triples stand in for the fields the connector's
//! virtual mapping attaches to fetched records at query time
//! (`specs/connector-competency/plan.md`: "Results carry source, record
//! ID, and as-of metadata ... from the virtual mapping").
//!
//! Time windows are expressed with the `YEAR()/MONTH()/DAY()/HOURS()/MINUTES()`
//! datetime accessors rather than lexical string comparisons: the engine's
//! ordering comparator parses operands as f64 (`eval_expr_bool_compare`,
//! `crates/medulla-rdf/src/sparql/expression.rs`), so `FILTER(STR(?t) >= "…")`
//! silently drops dateTime lexicals.
#![cfg(feature = "sparql")]

mod common;

use common::parse_all;
use medulla_core::graph::{GraphCore, GraphView};
use medulla_rdf::mapping::{load_triples, parse_turtle, IriStore};
use medulla_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

/// Load the assigned vocabulary documents and the fixture into one graph and
/// return the analysis view the SPARQL evaluator scans.
fn graph(schema: &[&str], fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    load_triples(&core, &mut iris, "fixture", parse_turtle(fixture).unwrap()).unwrap();
    core.analysis_snapshot()
}

/// Execute a SELECT and return `(columns, rows)` with every cell as its lexical form.
fn select(view: &GraphView, query: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let outcome = execute(
        &Dataset::new(view, Vec::new()),
        query,
        &Projection::raw(),
        None,
    )
    .expect("query executes");
    let table = match outcome {
        QueryOutcome::Solutions(table) => table,
        other => panic!("expected a SELECT solution table, got {other:?}"),
    };
    let (vars, rows) = table.to_rows();
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.expect("every selected variable binds"))
                .collect()
        })
        .collect();
    (vars, rows)
}

/// The local name of a bracketed node-id cell (`<urn:…/host-1>` → `host-1`).
fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

// spec: MEDULLA-CQ-CONN-R017.1
#[test]
fn egeria_confidential_glossary_terms_answer() {
    // "Which glossary terms are linked to a data asset with a confidential
    // classification?" — answered from the egeria connector alone. The
    // assigned module (core-foundation-v1.ttl) carries :classifiedAs and
    // :DataClassification; the glossary concept is the :GlossaryTerm /
    // :definesAsset pair already declared in the registered metadata_catalog
    // module (the assigned module's own GlossaryTerm producer rows,
    // R074.2-R074.3, have not landed). Frozen as-of: 2026-01-03T00:00:00Z.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix cat: <http://knuckles.team/kg/catalog#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:egeria:fixture/> .

# The classification markings this question asks about.
f:confidential a :DataClassification ;
    rdfs:label "Confidential" .
f:public a :DataClassification ;
    rdfs:label "Public" .

# The answer: a glossary term naming an asset classified Confidential.
f:term-confidential a cat:GlossaryTerm ;
    :definesAsset f:asset-orders ;
    dc:source "egeria" ;
    dc:identifier "egeria:rec:term-confidential" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:asset-orders a cat:Asset ;
    :classifiedAs f:confidential .

# Adversarial: a term whose asset is classified Public, not Confidential —
# no row.
f:term-public a cat:GlossaryTerm ;
    :definesAsset f:asset-metrics ;
    dc:source "egeria" ;
    dc:identifier "egeria:rec:term-public" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:asset-metrics a cat:Asset ;
    :classifiedAs f:public .

# Adversarial: a term that names no asset at all — no row.
f:term-orphan a cat:GlossaryTerm ;
    dc:source "egeria" ;
    dc:identifier "egeria:rec:term-orphan" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Adversarial: a Confidential asset no glossary term names — no row.
f:asset-orphan a cat:Asset ;
    :classifiedAs f:confidential .
"#;
    let view = graph(
        &[
            include_str!("../../medulla-core/ontology/core-foundation-v1.ttl"),
            include_str!("../../medulla-core/ontology/metadata_catalog-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX cat: <http://knuckles.team/kg/catalog#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX f: <urn:eg-cq-conn:egeria:fixture/>
SELECT ?term ?asset ?classification ?source ?record ?asof WHERE {
  ?term a cat:GlossaryTerm ;
        :definesAsset ?asset .
  ?asset a cat:Asset ;
         :classifiedAs ?classification .
  ?classification a :DataClassification .
  FILTER(?classification = f:confidential)
  ?term dc:source ?source ;
        dc:identifier ?record ;
        dc:date ?asof .
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_term = idx("term");
    let i_asset = idx("asset");
    let i_classification = idx("classification");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(rows.len(), 1, "exactly one glossary term answers: {rows:?}");
    let row = &rows[0];
    assert_eq!(local(&row[i_term]), "term-confidential");
    assert_eq!(local(&row[i_asset]), "asset-orders");
    assert_eq!(local(&row[i_classification]), "confidential");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "egeria");
    assert_eq!(row[i_record], "egeria:rec:term-confidential");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}

// spec: MEDULLA-CQ-CONN-R003.2
#[test]
fn archivebox_uncaptured_freshrss_pages_answer() {
    // "Which archived pages were first seen as FreshRSS articles but never
    // captured by ArchiveBox?" — federated fixture over the freshrss and
    // archivebox connectors, joined through the URL / web resource concept.
    // The assigned module (documentation-v1.ttl) has no web-snapshot
    // vocabulary on main yet (its producer rows, R071.x, are unlanded), so
    // the answer runs over the already-declared :WebResource / :WebCapture /
    // :FeedItem concepts in the registered retrieval module; a FreshRSS
    // article is the web resource it was first seen on, so the shared entity
    // is typed :WebResource, :FeedItem. The capture half is the native
    // NOT-EXISTS form: the native query path has no typed-abstention
    // mechanism, so an absent archivebox capture simply lets the page
    // answer. Frozen as-of: 2026-01-03T00:00:00Z.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:archivebox:fixture/> .

# The FreshRSS feed, fetched from the freshrss connector at query time.
f:feed-eng a :FeedSource ;
    rdfs:label "Engineering news" ;
    dc:source "freshrss" ;
    dc:identifier "freshrss:rec:feed-eng" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# The answer: a page first seen as a FreshRSS article, with no ArchiveBox
# capture on record.
f:page-rust a :WebResource, :FeedItem ;
    :itemOfFeed f:feed-eng ;
    dc:source "freshrss" ;
    dc:identifier "freshrss:rec:page-rust" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Adversarial: a page first seen as a FreshRSS article but already captured
# by ArchiveBox — the capture half of the question excludes it.
f:page-kernel a :WebResource, :FeedItem ;
    :itemOfFeed f:feed-eng ;
    dc:source "freshrss" ;
    dc:identifier "freshrss:rec:page-kernel" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:capture-kernel a :WebCapture ;
    :captureOf f:page-kernel ;
    dc:source "archivebox" ;
    dc:identifier "archivebox:rec:capture-kernel" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .

# Adversarial: an archived page never seen as a FreshRSS article — not a
# feed item, so no row.
f:page-solo a :WebResource .
f:capture-solo a :WebCapture ;
    :captureOf f:page-solo ;
    dc:source "archivebox" ;
    dc:identifier "archivebox:rec:capture-solo" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
"#;
    let view = graph(
        &[
            include_str!("../../medulla-core/ontology/retrieval-v1.ttl"),
            include_str!("../../medulla-core/ontology/documentation-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?page ?feed ?source ?record ?asof WHERE {
  ?page a :WebResource, :FeedItem ;
        :itemOfFeed ?feed .
  FILTER NOT EXISTS { ?capture a :WebCapture ; :captureOf ?page }
  ?page dc:source ?source ;
        dc:identifier ?record ;
        dc:date ?asof .
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_page = idx("page");
    let i_feed = idx("feed");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(
        rows.len(),
        1,
        "exactly one uncaptured page answers: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(local(&row[i_page]), "page-rust");
    assert_eq!(local(&row[i_feed]), "feed-eng");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "freshrss");
    assert_eq!(row[i_record], "freshrss:rec:page-rust");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}
