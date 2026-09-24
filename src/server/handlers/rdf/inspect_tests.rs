//! `OntologyInspect` over inline documents and the composed GraphSchema sources.

use super::*;

const KG: &str = "http://knuckles.team/kg#";
/// agent-utilities' compiled arr connector-manifest ontology and the canonical
/// digest AU pinned for it with rdflib (`provenance.integrity.hash`, 89 triples).
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
    let documents: Vec<String> = documents.iter().map(|document| document.to_string()).collect();
    inspect(&GraphCore::new(), &documents, &[])
}

#[test]
fn a_compiled_connector_manifest_keeps_its_pinned_canonical_digest() {
    let view = inline(&[ARR_MANIFEST]).unwrap();
    assert_eq!(view.canonical_digest.as_deref(), Some(ARR_PINNED_DIGEST));
    assert_eq!(view.triple_count, 89);
    assert_eq!(view.schema_digests.len(), 1);
    assert_eq!(view.composed_digest, None);
    assert_eq!(view.ontologies, ["http://knuckles.team/kg/arr"]);
    let album = view.classes.iter().find(|class| class.iri == kg("Album")).unwrap();
    assert_eq!(album.label.as_deref(), Some("Album"));
}

#[test]
fn the_vocabulary_view_names_classes_properties_and_targets() {
    let view = inline(&[VOCABULARY]).unwrap();
    let classes: Vec<&str> = view.classes.iter().map(|class| class.iri.as_str()).collect();
    assert_eq!(classes, [kg("Agent"), kg("Planner")]);
    assert_eq!(view.classes[0].comment.as_deref(), Some("An actor."));
    assert_eq!(view.classes[1].parents, [kg("Agent")]);

    let peer = &view.object_properties[0];
    assert_eq!(peer.iri, kg("peerOf"));
    assert_eq!(peer.label.as_deref(), Some("peer of"));
    assert_eq!((peer.domains.clone(), peer.ranges.clone()), (vec![kg("Agent")], vec![kg("Agent")]));
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
    assert_ne!(inline(&[&grown]).unwrap().canonical_digest, turtle.canonical_digest);
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
fn the_composed_schema_carries_the_agent_orchestration_shapes() {
    let core = GraphCore::new();
    let all = inspect(&core, &[], &[]).unwrap();
    assert!(all.composed_digest.is_some());
    for target in ["HarnessEdit", "WorkflowDefinition", "TemporalFact", "Recommendation"] {
        assert!(all.shape_target_classes.contains(&kg(target)), "{target}");
    }
    assert!(all.classes.iter().any(|class| class.iri == kg("Agent")));

    let harness = inspect(&core, &[], &["core:harness-shapes@1".to_string()]).unwrap();
    let targets: Vec<String> = ["HarnessDimension", "HarnessEdit", "HarnessVariant", "Processor"]
        .iter()
        .map(|local| kg(local))
        .collect();
    assert_eq!(harness.shape_target_classes, targets);
    assert_eq!(
        harness.schema_digests,
        ["9adf8d076018d2bb02c80e0a8816c4bc29676f1a2263799755fa1e11e3d33191"]
    );
}

#[test]
fn ambiguous_unknown_and_malformed_requests_are_refused() {
    let core = GraphCore::new();
    let both = inspect(&core, &[VOCABULARY.to_string()], &["core:catalog@1".to_string()]);
    assert!(both.unwrap_err().contains("not both"));
    let unknown = inspect(&core, &[], &["core:no-such@1".to_string()]);
    assert!(unknown.unwrap_err().contains("unknown GraphSchema source 'core:no-such@1'"));
    let malformed = inline(&["this is not turtle ."]);
    assert!(malformed.unwrap_err().starts_with("documents[0] is not Turtle"));
}
