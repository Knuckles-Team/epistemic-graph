//! `OntologyInspect` over inline documents and the composed GraphSchema sources.

use super::*;

const KG: &str = "http://knuckles.team/kg#";
/// A compiled connector-manifest ontology fixture and its independently
/// computed canonical digest (89 triples).
const ARR_MANIFEST: &str =
    include_str!("../../../../tests/fixtures/ontology_inspect/arr_connector_manifest.ttl");
const ARR_PINNED_DIGEST: &str = "8b921c452bb35f134b2d8dad809afcb49ec7abd16d78f5ee79e704ae5acb7c75";

const VOCABULARY: &str = "@prefix : <http://knuckles.team/kg#> .\n\
    @prefix owl: <http://www.w3.org/2002/07/owl#> .\n\
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
    @prefix sh: <http://www.w3.org/ns/shacl#> .\n\
    :Agent a owl:Class ; rdfs:label \"Agent\" ; rdfs:comment \"An actor.\" .\n\
    :Planner a owl:Class ; rdfs:subClassOf :Agent .\n\
    :peerOf a owl:ObjectProperty, owl:SymmetricProperty ; rdfs:label \"peer of\" ;\n\
        rdfs:domain :Agent ; rdfs:range :Agent .\n\
    :name a owl:DatatypeProperty ; rdfs:range xsd:string .\n\
    :AgentShape a sh:NodeShape ; sh:targetClass :Agent .\n";

fn kg(local: &str) -> String {
    format!("{KG}{local}")
}

fn inline(documents: &[&str]) -> Result<OntologyInspection, String> {
    let documents: Vec<String> = documents
        .iter()
        .map(|document| document.to_string())
        .collect();
    inspect(&GraphCore::new(), &documents, &[])
}

// spec: EG-TYPED-PACKS-R098
#[test]
fn a_compiled_connector_manifest_keeps_its_pinned_canonical_digest() {
    let view = inline(&[ARR_MANIFEST]).unwrap();
    assert_eq!(view.canonical_digest.as_deref(), Some(ARR_PINNED_DIGEST));
    assert_eq!(view.triple_count, 89);
    assert_eq!(view.schema_digests.len(), 1);
    assert_eq!(view.composed_digest, None);
    assert_eq!(view.ontologies, ["http://knuckles.team/kg/arr"]);
    let album = view
        .classes
        .iter()
        .find(|class| class.iri == kg("Album"))
        .unwrap();
    assert_eq!(album.label.as_deref(), Some("Album"));
}

// spec: EG-TYPED-PACKS-R098
#[test]
fn the_vocabulary_view_names_classes_properties_and_targets() {
    let view = inline(&[VOCABULARY]).unwrap();
    let classes: Vec<&str> = view
        .classes
        .iter()
        .map(|class| class.iri.as_str())
        .collect();
    assert_eq!(classes, [kg("Agent"), kg("Planner")]);
    assert_eq!(view.classes[0].comment.as_deref(), Some("An actor."));
    assert_eq!(view.classes[1].parents, [kg("Agent")]);

    let peer = &view.object_properties[0];
    assert_eq!(peer.iri, kg("peerOf"));
    assert_eq!(peer.label.as_deref(), Some("peer of"));
    assert_eq!(
        (peer.domains.clone(), peer.ranges.clone()),
        (vec![kg("Agent")], vec![kg("Agent")])
    );
    assert!(peer.symmetric);

    let name = &view.datatype_properties[0];
    assert_eq!(name.ranges, ["http://www.w3.org/2001/XMLSchema#string"]);
    assert!(!name.symmetric);
    assert_eq!(view.shape_target_classes, [kg("Agent")]);
    assert!(view.canonical_digest.is_some());
}

#[test]
fn the_canonical_digest_ignores_serialization_and_tracks_content() {
    let turtle = inline(&[VOCABULARY]).unwrap();
    let lines: Vec<String> = eg_rdf::mapping::parse_turtle(VOCABULARY)
        .unwrap()
        .iter()
        .rev()
        .map(|triple| format!("{triple} ."))
        .collect();
    let ntriples = inline(&[&lines.join("\n")]).unwrap();
    assert_eq!(turtle.canonical_digest, ntriples.canonical_digest);
    assert_eq!(turtle.triple_count, ntriples.triple_count);
    let grown = format!("{VOCABULARY}:Gamma a owl:Class .\n");
    assert_ne!(
        inline(&[&grown]).unwrap().canonical_digest,
        turtle.canonical_digest
    );
}

#[test]
fn a_document_with_blank_nodes_has_no_canonical_digest() {
    let shapes = "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
                  @prefix : <http://knuckles.team/kg#> .\n\
                  :S a sh:NodeShape ; sh:targetClass :Agent ; sh:property [ sh:path :name ] .\n";
    let view = inline(&[shapes]).unwrap();
    assert_eq!(view.canonical_digest, None);
    assert_eq!(view.shape_target_classes, [kg("Agent")]);
}

#[test]
fn the_composed_schema_reads_the_live_core_catalog() {
    let core = GraphCore::new();
    let all = inspect(&core, &[], &[]).unwrap();
    assert!(all.composed_digest.is_some());
    assert!(all.triple_count > 0);
    assert!(
        all.ontologies
            .contains(&"http://knuckles.team/kg".to_string()),
        "{:?}",
        all.ontologies
    );

    // A named-source filter reads a strict subset of the same composed catalog
    // (same composed digest, fewer triples than the unfiltered read).
    let catalog = inspect(&core, &[], &["core:catalog@1".to_string()]).unwrap();
    assert_eq!(catalog.schema_digests.len(), 1);
    assert_eq!(catalog.composed_digest, all.composed_digest);
    assert!(catalog.triple_count <= all.triple_count);
}

#[test]
fn ambiguous_unknown_and_malformed_requests_are_refused() {
    let core = GraphCore::new();
    let both = inspect(
        &core,
        &[VOCABULARY.to_string()],
        &["core:catalog@1".to_string()],
    );
    assert!(both.unwrap_err().contains("not both"));
    let unknown = inspect(&core, &[], &["core:no-such@1".to_string()]);
    assert!(unknown
        .unwrap_err()
        .contains("unknown GraphSchema source 'core:no-such@1'"));
    let malformed = inline(&["this is not turtle ."]);
    assert!(malformed
        .unwrap_err()
        .starts_with("documents[0] is not Turtle"));
}

#[test]
fn literal_lexical_forms_never_become_named_vocabulary_links() {
    let view = inline(&[r#"
        @prefix : <http://ex/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        :FakeClass a "http://www.w3.org/2002/07/owl#Class" .
        :FakeOntology a "http://www.w3.org/2002/07/owl#Ontology" .
        :FakeObject a "http://www.w3.org/2002/07/owl#ObjectProperty" .
        :FakeDatatype a "http://www.w3.org/2002/07/owl#DatatypeProperty" .
        :C a owl:Class ; rdfs:subClassOf :Parent, "http://ex/FakeParent" ;
            rdfs:label "Class label" ; rdfs:comment "Class comment" .
        :p a owl:ObjectProperty, "http://www.w3.org/2002/07/owl#SymmetricProperty" ;
            rdfs:domain :C, "http://ex/FakeDomain" ;
            rdfs:range :Parent, "http://ex/FakeRange" .
        :s sh:targetClass :C, "http://ex/FakeTarget" .
    "#])
    .unwrap();
    assert!(view.ontologies.is_empty());
    assert!(view.datatype_properties.is_empty());
    assert_eq!(view.classes.len(), 1);
    assert_eq!(view.classes[0].iri, "http://ex/C");
    assert_eq!(view.classes[0].parents, ["http://ex/Parent"]);
    assert_eq!(view.classes[0].label.as_deref(), Some("Class label"));
    assert_eq!(view.classes[0].comment.as_deref(), Some("Class comment"));
    assert_eq!(view.object_properties.len(), 1);
    assert_eq!(view.object_properties[0].domains, ["http://ex/C"]);
    assert_eq!(view.object_properties[0].ranges, ["http://ex/Parent"]);
    assert!(!view.object_properties[0].symmetric);
    assert_eq!(view.shape_target_classes, ["http://ex/C"]);
}

#[test]
fn inspection_rejects_document_count_and_bytes_before_parsing() {
    let documents = vec![String::new(); MAX_INSPECT_DOCUMENTS + 1];
    assert!(inspect(&GraphCore::new(), &documents, &[])
        .unwrap_err()
        .contains("documents exceed"));
    let bytes = eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES;
    // An over-limit malformed document must fail its byte budget before syntax.
    let oversized = "!".repeat(bytes + 1);
    assert!(inline(&[&oversized])
        .unwrap_err()
        .contains(&format!("maximum of {bytes} bytes")));
    let boundary = " ".repeat(bytes);
    assert_eq!(inline(&[&boundary]).unwrap().triple_count, 0);
    assert!(inspect(&GraphCore::new(), &documents[..MAX_INSPECT_DOCUMENTS], &[]).is_ok());
}

#[test]
fn inspection_stops_at_the_first_triple_over_budget() {
    let limit = crate::graph::MAX_SCHEMA_DOCUMENT_TRIPLES;
    let boundary = format!("@prefix : <x:> .\n{}", ":a :b :c .\n".repeat(limit));
    assert_eq!(inline(&[&boundary]).unwrap().triple_count, 1);
    // Stay below the byte bound and place invalid syntax after the over-limit
    // triple: a parser that eagerly drains the document reports syntax instead.
    let oversized = format!("{boundary}:a :b :c .\nthis is not Turtle .");
    assert!(oversized.len() < eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES);
    let error = inline(&[&oversized]).unwrap_err();
    assert!(
        error.contains(&format!("maximum of {limit} triples")),
        "{error}"
    );
}
