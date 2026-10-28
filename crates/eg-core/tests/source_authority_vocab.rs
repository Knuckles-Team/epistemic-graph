//! The source-authority, source-role and trust-mark terms are part of the core
//! virtual-graph schema source (EG-SRC-AUTH-R001, R010.1, R011.1).

use eg_core::graph::GraphSchemaSources;
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";

/// The triples of the composed core virtual-graph source, read through the
/// default schema-source catalog rather than the file, so the test proves the
/// terms reach every graph.
fn core_vocabulary() -> Vec<Triple> {
    let sources = GraphSchemaSources::default();
    let source = sources
        .core
        .get("core:virtual-graph@1")
        .expect("the virtual-graph core source");
    parse_turtle(source.ontology_ttl.as_deref().expect("ontology text")).expect("parses")
}

fn typed(triples: &[Triple], local: &str, kind: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(format!("{VG}{local}")).unwrap(),
        NamedNode::new(RDF_TYPE).unwrap(),
        NamedNode::new(format!("{OWL}{kind}")).unwrap(),
    ))
}

fn domain(triples: &[Triple], local: &str, class: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(format!("{VG}{local}")).unwrap(),
        NamedNode::new(RDFS_DOMAIN).unwrap(),
        NamedNode::new(format!("{VG}{class}")).unwrap(),
    ))
}

fn assert_terms(terms: &[(&str, &str)]) {
    let triples = core_vocabulary();
    for (local, kind) in terms {
        assert!(typed(&triples, local, kind), "vg:{local} a owl:{kind}");
    }
}

// spec: EG-SRC-AUTH-R001.1
#[test]
fn the_authority_terms_are_in_the_core_vocabulary() {
    assert_terms(&[
        ("SourceAuthority", "Class"),
        ("authoritativeSource", "ObjectProperty"),
        ("authorityFor", "ObjectProperty"),
        ("authorityRank", "DatatypeProperty"),
    ]);
}

// spec: EG-SRC-AUTH-R001.2
#[test]
fn the_authority_facet_and_mapping_terms_exist() {
    assert_terms(&[
        ("authorityFacet", "ObjectProperty"),
        ("authorityMapping", "ObjectProperty"),
    ]);
    assert!(domain(
        &core_vocabulary(),
        "authorityMapping",
        "SourceAuthority"
    ));
}

// spec: EG-SRC-AUTH-R001.3
#[test]
fn the_authority_scope_and_freshness_terms_exist() {
    assert_terms(&[
        ("scopeTenant", "DatatypeProperty"),
        ("scopePartition", "DatatypeProperty"),
        ("freshnessMaxAgeSeconds", "DatatypeProperty"),
    ]);
}

// spec: EG-SRC-AUTH-R001.4
#[test]
fn the_authority_contract_and_fallback_terms_exist() {
    assert_terms(&[
        ("authorityContract", "DatatypeProperty"),
        ("allowFallback", "DatatypeProperty"),
    ]);
}

// spec: EG-SRC-AUTH-R001.5
#[test]
fn the_materialisation_terms_exist() {
    assert_terms(&[
        ("materialisation", "DatatypeProperty"),
        ("materialisationReason", "DatatypeProperty"),
    ]);
}

// spec: EG-SRC-AUTH-R001.6
#[test]
fn the_foreign_source_name_and_widened_approved_exist() {
    let triples = core_vocabulary();
    assert!(typed(&triples, "foreignSourceName", "DatatypeProperty"));
    assert!(domain(&triples, "foreignSourceName", "SourceConnection"));
    // vg:approved now ranges over the union of VirtualMapping and SourceAuthority:
    // its domain is no longer the bare VirtualMapping class, and the union names
    // SourceAuthority.
    assert!(!domain(&triples, "approved", "VirtualMapping"));
    let text = GraphSchemaSources::default().core["core:virtual-graph@1"]
        .ontology_ttl
        .clone()
        .unwrap();
    assert!(text.contains("owl:unionOf ( vg:VirtualMapping vg:SourceAuthority )"));
}

// spec: EG-SRC-AUTH-R010.1
#[test]
fn the_source_role_term_exists() {
    let triples = core_vocabulary();
    assert!(typed(&triples, "sourceRole", "DatatypeProperty"));
    assert!(domain(&triples, "sourceRole", "SourceConnection"));
}

// spec: EG-SRC-AUTH-R011.1
#[test]
fn the_trust_mark_term_exists() {
    assert_terms(&[("trustMark", "DatatypeProperty")]);
}
