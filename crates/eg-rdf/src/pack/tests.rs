use super::*;
use crate::oxrdf::{NamedOrBlankNode, Term};

#[test]
fn malformed_documents_preserve_the_callers_refusal_code() {
    for code in [
        PackViolationCode::OntologyInvalid,
        PackViolationCode::ShapesInvalid,
    ] {
        assert_eq!(
            validate_document(code, "not Turtle"),
            Err((code, "RDF body is not valid Turtle"))
        );
    }
}

#[test]
fn triple_budget_boundary_is_inclusive() {
    let triple = "<urn:s> <urn:p> <urn:o> .\n";
    let mut text = triple.repeat(100_000);
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &text),
        Ok(())
    );
    text.push_str(triple);
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &text),
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "RDF graph exceeds the triple validation budget",
        ))
    );
}

#[test]
fn literal_bound_is_measured_in_utf8_bytes() {
    let valid = format!("<urn:s> <urn:p> \"{}\" .", "é".repeat(128 * 1024));
    assert_eq!(
        validate_document(PackViolationCode::ShapesInvalid, &valid),
        Ok(())
    );
    let invalid = valid.replacen("\" .", "x\" .", 1);
    assert_eq!(
        validate_document(PackViolationCode::ShapesInvalid, &invalid),
        Err((
            PackViolationCode::ShapesInvalid,
            "RDF graph contains a literal above the served bound",
        ))
    );
}

#[test]
fn iri_bound_preserves_term_specific_measurement() {
    // Subject bound includes its rendered angle brackets; object bound does not.
    let iri = format!("urn:{}", "x".repeat(4092));
    let object = format!("<urn:s> <urn:p> <{iri}> .");
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &object),
        Ok(())
    );
    let subject = format!("<{iri}> <urn:p> <urn:o> .");
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &subject),
        Err((
            PackViolationCode::OntologyInvalid,
            "RDF graph contains an IRI above the served bound",
        ))
    );
}

#[test]
fn imports_allow_only_other_supplied_pack_identities() {
    let allowed = BTreeSet::from(["urn:current", "urn:other"]);
    for (target, expected) in [
        ("<urn:other>", Ok(())),
        (
            "<urn:current>",
            Err("owl:imports may name only an ontology entry in this pack"),
        ),
        (
            "<https://example.invalid/remote>",
            Err("owl:imports may name only an ontology entry in this pack"),
        ),
        (
            "_:blank",
            Err("owl:imports must name an ontology IRI from this pack"),
        ),
        (
            "\"urn:other\"",
            Err("owl:imports must name an ontology IRI from this pack"),
        ),
    ] {
        let text = format!("<urn:current> <http://www.w3.org/2002/07/owl#imports> {target} .");
        assert_eq!(
            validate_ontology_imports(&text, "urn:current", &allowed),
            expected
        );
    }
    assert_eq!(
        validate_ontology_imports("bad", "urn:current", &allowed),
        Err("ontology import policy could not parse its graph")
    );
}

#[test]
fn named_shape_discovery_excludes_blank_shapes_and_other_classes() {
    let text = "@prefix sh: <http://www.w3.org/ns/shacl#> .
        <urn:node> a sh:NodeShape . <urn:property> a sh:PropertyShape .
        _:blank a sh:NodeShape . <urn:other> a <urn:Other> .";
    assert_eq!(
        declared_shape_iris(text),
        BTreeSet::from(["urn:node".into(), "urn:property".into()])
    );
    assert!(declared_shape_iris("bad").is_empty());
}

#[test]
fn union_keeps_document_order_prefixes_and_blank_node_references() {
    let documents = [
        "@prefix p: <urn:first:> . _:same p:edge _:same .".into(),
        "@prefix p: <urn:second:> . _:same p:edge _:same .".into(),
    ];
    let union = scoped_union(&documents).unwrap();
    for (index, triple) in union.triples.iter().enumerate() {
        let NamedOrBlankNode::BlankNode(subject) = &triple.subject else {
            panic!("blank subject")
        };
        let Term::BlankNode(object) = &triple.object else {
            panic!("blank object")
        };
        assert_eq!(subject, object);
        assert_eq!(subject.as_str(), format!("f{index}xsame"));
    }
    assert_eq!(union.triples[0].predicate.as_str(), "urn:first:edge");
    assert_eq!(union.triples[1].predicate.as_str(), "urn:second:edge");
    assert_eq!(
        crate::mapping::parse_ntriples(&union.ntriples).unwrap(),
        union.triples
    );
    assert!(scoped_union(&[]).unwrap().triples.is_empty());
}

// spec: EG-TYPED-PACKS-R105.1
#[test]
fn imports_may_name_the_parent_catalog_and_its_modules() {
    let allowed = BTreeSet::from(["urn:current"]);
    for parent in ["http://knuckles.team/kg", "http://knuckles.team/kg/core"] {
        let text = format!("<urn:current> <http://www.w3.org/2002/07/owl#imports> <{parent}> .");
        assert_eq!(
            validate_ontology_imports(&text, "urn:current", &allowed),
            Ok(())
        );
    }
    assert!(!is_parent_ontology_iri("http://knuckles.team/kg/core/x"));
    assert!(!is_parent_ontology_iri("http://knuckles.team/kgx"));
}

// spec: EG-TYPED-PACKS-R105.1
#[test]
fn orphan_classes_are_those_without_a_path_to_a_parent_class() {
    let parent = BTreeSet::from(["http://knuckles.team/kg#Event".to_string()]);
    let linked = "@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix c: <urn:connector#> .
c:Incident a owl:Class ; rdfs:subClassOf c:Ticket .
c:Ticket a owl:Class ; rdfs:subClassOf <http://knuckles.team/kg#Event> .
";
    let orphan = "@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix c: <urn:connector#> .
c:Widget a owl:Class .
<http://knuckles.team/kg#Event> a owl:Class .
";
    assert!(ontology_orphan_classes(&[linked], &parent).is_empty());
    assert_eq!(
        ontology_orphan_classes(&[linked, orphan], &parent),
        vec!["urn:connector#Widget".to_string()]
    );
    // A path that runs through another pack document still links.
    let split = "@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<urn:connector#Gadget> a owl:Class ; rdfs:subClassOf <urn:connector#Ticket> .
";
    assert!(ontology_orphan_classes(&[split, linked], &parent).is_empty());
}
