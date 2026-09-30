//! Regression controls for review findings on the shared hardening boundary.
use eg_shacl::{validate_icv_turtle, validate_turtle};

const PREFIX: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <urn:ex:> .";

#[test]
fn multiple_classes_are_conjunctive_constraints() {
    let shapes = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:class ex:A, ex:B .");
    for (types, conforms) in [("ex:A, ex:B", true), ("ex:A", false), ("ex:B", false)] {
        let data = format!("{PREFIX} ex:x a {types} .");
        assert_eq!(validate_turtle(&shapes, &data).unwrap().conforms, conforms);
        assert_eq!(
            validate_icv_turtle(&shapes, &data).unwrap().conforms,
            conforms
        );
    }
}

#[test]
fn shapes_graph_well_formed_is_report_metadata() {
    let shapes = format!("{PREFIX} ex:S sh:targetNode ex:x . ex:Report a sh:ValidationReport ; sh:shapesGraphWellFormed true .");
    assert!(validate_turtle(&shapes, "").unwrap().conforms);
    assert!(validate_icv_turtle(&shapes, "").unwrap().conforms);
}

#[test]
fn indexed_class_checks_do_not_charge_the_whole_data_graph() {
    let shapes = format!("{PREFIX} ex:S sh:targetClass ex:A ; sh:class ex:A .");
    assert_engines_conform(&shapes, &class_instances(4000));
}

#[test]
fn repeatable_constraints_preserve_every_parameter() {
    for (predicate, values, data, conforms) in [
        ("hasValue", "ex:a, ex:b", "ex:x ex:p ex:a, ex:b .", true),
        ("hasValue", "ex:a, ex:b", "ex:x ex:p ex:a .", false),
        ("hasValue", "ex:a, ex:b", "ex:x ex:p ex:b .", false),
        ("pattern", "\"a\", \"b\"", "ex:x ex:p \"ab\" .", true),
        ("pattern", "\"a\", \"b\"", "ex:x ex:p \"a\" .", false),
        ("not", "ex:A, ex:B", "ex:x ex:p ex:z . ex:z a ex:C .", true),
        ("not", "ex:A, ex:B", "ex:x ex:p ex:z . ex:z a ex:B .", false),
        (
            "and",
            "(ex:A), (ex:B)",
            "ex:x ex:p ex:z . ex:z a ex:A, ex:B .",
            true,
        ),
        (
            "and",
            "(ex:A), (ex:B)",
            "ex:x ex:p ex:z . ex:z a ex:A .",
            false,
        ),
        (
            "or",
            "(ex:A), (ex:B)",
            "ex:x ex:p ex:z . ex:z a ex:A .",
            false,
        ),
        (
            "xone",
            "(ex:A), (ex:B)",
            "ex:x ex:p ex:z . ex:z a ex:B .",
            false,
        ),
    ] {
        let shapes = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:path ex:p ; sh:{predicate} {values} . ex:A sh:class ex:A . ex:B sh:class ex:B .");
        let data = format!("{PREFIX} {data}");
        assert_eq!(
            validate_turtle(&shapes, &data).unwrap().conforms,
            conforms,
            "{predicate}: {data}"
        );
        assert_eq!(
            validate_icv_turtle(&shapes, &data).unwrap().conforms,
            conforms,
            "ICV {predicate}: {data}"
        );
    }
}

#[test]
fn unrelated_metadata_does_not_multiply_indexed_shape_parse_cost() {
    let mut shapes = format!(
        "{PREFIX} ex:S sh:targetClass ex:A ; sh:node ex:Required . ex:Required sh:class ex:A ."
    );
    for i in 0..4000 {
        shapes.push_str(&format!("ex:metadata{i} ex:label \"metadata\" .\n"));
    }
    assert_engines_conform(&shapes, &class_instances(4000));
}

#[test]
fn repeated_optional_flags_and_ignored_lists_are_conjunctive() {
    let pattern = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:path ex:p ; sh:pattern \"A\" ; sh:flags \"i\", \"\" .");
    assert!(
        !validate_turtle(&pattern, "<urn:ex:x> <urn:ex:p> \"a\" .")
            .unwrap()
            .conforms
    );
    assert!(
        validate_turtle(&pattern, "<urn:ex:x> <urn:ex:p> \"A\" .")
            .unwrap()
            .conforms
    );
    let closed = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:closed true, false ; sh:ignoredProperties (ex:p), (ex:p ex:q) .");
    assert!(
        validate_turtle(&closed, "<urn:ex:x> <urn:ex:p> 1 .")
            .unwrap()
            .conforms
    );
    assert!(
        !validate_turtle(&closed, "<urn:ex:x> <urn:ex:q> 1 .")
            .unwrap()
            .conforms
    );
}

#[test]
fn icv_does_not_label_a_repeated_parameter_failure_with_the_first_parameter() {
    let shapes = format!("{PREFIX} ex:S sh:targetNode ex:x ; sh:class ex:A, ex:B .");
    let data = format!("{PREFIX} ex:x a ex:A .");
    let report = validate_icv_turtle(&shapes, &data).unwrap();
    assert!(!report.conforms);
    assert_eq!(report.violations.len(), 1);
    assert!(!report.violations[0].witness.contains("?value a <urn:ex:A>"));
}

fn class_instances(count: usize) -> String {
    (0..count)
        .map(|i| format!("<urn:ex:n{i}> a <urn:ex:A> .\n"))
        .collect()
}

fn assert_engines_conform(shapes: &str, data: &str) {
    assert!(validate_turtle(shapes, data).unwrap().conforms);
    assert!(validate_icv_turtle(shapes, data).unwrap().conforms);
}
