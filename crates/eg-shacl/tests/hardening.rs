//! Small before/after fixtures for shared SHACL evaluation hardening.
use eg_shacl::{validate_icv_turtle, validate_turtle};

const PREFIX: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <urn:ex:> .";

fn chain(depth: usize) -> String {
    let mut shapes = format!("{PREFIX} ex:S0 sh:targetNode ex:x .\n");
    for i in 0..depth {
        shapes.push_str(&format!("ex:S{i} sh:node ex:S{} .\n", i + 1));
    }
    shapes.push_str(&format!("ex:S{depth} sh:in () ."));
    shapes
}

fn branching(depth: usize) -> String {
    let mut shapes = format!("{PREFIX} ex:S0 sh:targetNode ex:x .\n");
    for i in 0..depth {
        shapes.push_str(&format!(
            "ex:S{i} sh:and ( ex:S{} ex:S{} ) .\n",
            i + 1,
            i + 1
        ));
    }
    shapes
}

#[test]
fn depth_exhaustion_is_a_refusal_in_shacl_and_icv() {
    assert!(!validate_turtle(&chain(1), "").unwrap().conforms);
    assert_eq!(
        validate_turtle(&chain(45), "").unwrap_err(),
        eg_shacl::DEPTH_EXCEEDED
    );
    assert_eq!(
        validate_icv_turtle(&chain(45), "").unwrap_err(),
        eg_shacl::DEPTH_EXCEEDED
    );
}

#[test]
fn unsupported_semantic_predicate_is_refused_in_shacl_and_icv() {
    let shape = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:path ex:p ; sh:equals ex:q .");
    let data = "<urn:ex:x> <urn:ex:p> <urn:ex:y> .";
    assert!(validate_turtle(&shape, data)
        .unwrap_err()
        .contains("predicate is not supported"));
    assert!(validate_icv_turtle(&shape, data)
        .unwrap_err()
        .contains("predicate is not supported"));
}

#[test]
fn tiny_shape_graph_amplifies_work_beyond_graph_size_estimate() {
    // Both branches conform, so sh:and must evaluate both references at every
    // level: 8,191 shape evaluations for only 61 shape triples and no data.
    let shapes = branching(12);
    assert!(validate_turtle(&shapes, "").unwrap().conforms);
    assert_eq!(
        validate_turtle(&branching(18), "").unwrap_err(),
        eg_shacl::WORK_EXCEEDED
    );
    assert_eq!(
        validate_icv_turtle(&branching(18), "").unwrap_err(),
        eg_shacl::WORK_EXCEEDED
    );
}

#[test]
fn legitimate_annotations_do_not_change_conformance() {
    let shape = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:name \"Name\" ; sh:description \"Description\" ; sh:order 1 ; sh:group ex:Group ; ex:annotation \"custom metadata\" ; <http://www.w3.org/2000/01/rdf-schema#label> \"label\" .");
    assert!(validate_turtle(&shape, "").unwrap().conforms);
}

#[test]
fn unsupported_constructs_and_malformed_parameters_fail_before_target_selection() {
    for clause in [
        "sh:path (ex:p ex:q)",
        "sh:qualifiedMinCount 1",
        "sh:uniqueLang true",
        "sh:target [ a sh:SPARQLTarget ]",
        "sh:nodeKind ex:Typo",
        "sh:minCount -1",
        "sh:minCount 1, 2",
        "sh:closed \"true\"",
        "sh:and ex:Incomplete",
        "sh:languageIn (ex:NotALiteral)",
        "sh:property \"not a shape\"",
        "a sh:ConstraintComponent",
    ] {
        let shapes = format!("{PREFIX} ex:S {clause} .");
        assert!(validate_turtle(&shapes, "").is_err(), "accepted {clause}");
    }
}

#[test]
fn cyclic_list_is_a_refusal() {
    let shapes = format!("{PREFIX} @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . ex:S sh:and _:list . _:list rdf:first ex:S ; rdf:rest _:list .");
    assert!(validate_turtle(&shapes, "")
        .unwrap_err()
        .contains("cyclic RDF list"));
}

#[test]
fn depth_boundary_and_cycle_are_fail_closed() {
    assert!(!validate_turtle(&chain(40), "").unwrap().conforms);
    assert_eq!(
        validate_turtle(&chain(41), "").unwrap_err(),
        eg_shacl::DEPTH_EXCEEDED
    );
    let cycle = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:node ex:S .");
    assert_eq!(
        validate_turtle(&cycle, "").unwrap_err(),
        eg_shacl::DEPTH_EXCEEDED
    );
}

#[test]
fn explicit_focus_uses_the_same_refusals() {
    let graph = eg_shacl::graph_from_turtle(&chain(45)).unwrap();
    let data = eg_shacl::graph_from_turtle("").unwrap();
    use eg_rdf::oxrdf::{NamedNode, Term};
    let focus = Term::NamedNode(NamedNode::new_unchecked("urn:ex:x"));
    assert_eq!(
        eg_shacl::validate_nodes(&graph, &data, "urn:ex:S0", &[focus]).unwrap_err(),
        eg_shacl::DEPTH_EXCEEDED
    );
}

#[test]
fn boolean_one_activates_closed_constraint() {
    let shapes = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:closed \"1\"^^<http://www.w3.org/2001/XMLSchema#boolean> .");
    assert!(
        !validate_turtle(&shapes, "<urn:ex:x> <urn:ex:p> <urn:ex:y> .")
            .unwrap()
            .conforms
    );
}

#[test]
fn write_check_refuses_uncheckable_policy() {
    let shapes = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:equals ex:q .");
    let policy = eg_shacl::IcvPolicy::from_turtle(&shapes).unwrap();
    assert!(policy
        .check(&eg_shacl::Graph::new(), &[], &[])
        .unwrap_err()
        .contains("predicate is not supported"));
}

#[test]
fn in_graph_prefix_imports_are_not_silently_depth_truncated() {
    let mut shapes = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:sparql [ sh:prefixes ex:P0 ; sh:select \"SELECT $this WHERE {{ FILTER($this = ex:x) }}\" ] .");
    for i in 0..25 {
        shapes.push_str(&format!(
            "ex:P{i} <http://www.w3.org/2002/07/owl#imports> ex:P{} .",
            i + 1
        ));
    }
    shapes.push_str("ex:P25 sh:declare [ sh:prefix \"ex\" ; sh:namespace \"urn:ex:\" ] .");
    assert!(!validate_turtle(&shapes, "").unwrap().conforms);
}
