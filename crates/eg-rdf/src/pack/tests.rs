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
    let triple = "<s:s><p:p><o:o>.";
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
        crate::mapping::parse_turtle(&union.ntriples).unwrap(),
        union.triples
    );
    assert!(scoped_union(&[]).unwrap().triples.is_empty());
}

#[test]
fn oversized_document_is_refused_before_parsing() {
    let text = "!".repeat(eg_types::connector_pack::MAX_PACK_BODY_BYTES as usize + 1);
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &text),
        Err((
            PackViolationCode::PackTooLarge,
            "section exceeds its served size bound"
        ))
    );
}

#[test]
fn streamed_term_refusal_precedes_later_malformed_turtle() {
    let text = format!("<urn:s> <urn:p> <urn:{}> . not Turtle", "x".repeat(4093));
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &text),
        Err((
            PackViolationCode::OntologyInvalid,
            "RDF graph contains an IRI above the served bound"
        ))
    );
}

#[test]
fn bounded_union_refuses_expansion_and_reports_document_index() {
    let documents = [
        "<urn:s> <urn:p> <urn:o> .".into(),
        "<urn:s> <urn:p> <urn:o> .".into(),
    ];
    let bytes = scoped_union(&documents[..1]).unwrap().ntriples.len();
    assert_eq!(
        scoped_union_with_limits(&documents[..1], 1, bytes)
            .unwrap()
            .triples
            .len(),
        1
    );
    assert_eq!(
        scoped_union_with_limits(&documents, 1, usize::MAX).err(),
        Some(UnionLimitError::Triples { file: 1 })
    );
    assert_eq!(
        scoped_union_with_limits(&documents, 2, bytes).err(),
        Some(UnionLimitError::RenderedBytes { file: 1 })
    );
    let malformed = [documents[0].clone(), "bad".into()];
    assert_eq!(
        scoped_union_with_limits(&malformed, 2, usize::MAX).err(),
        Some(UnionLimitError::Parse { file: 1 })
    );
}

#[test]
fn compact_prefix_expansion_is_bounded_before_later_pack_passes() {
    let prefix = format!("@prefix : <urn:{}> .\n", "x".repeat(1020));
    let text = prefix + &":s :p :o .\n".repeat(6000);
    assert!(text.len() < 100_000);
    assert_eq!(
        validate_document(PackViolationCode::OntologyInvalid, &text),
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "RDF graph rendering exceeds the byte validation budget"
        ))
    );
}
