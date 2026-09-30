use super::*;

const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const SH: &str = "http://www.w3.org/ns/shacl#";

fn code(result: Result<(), Refusal>) -> PackViolationCode {
    result.unwrap_err().0
}

#[test]
fn empty_and_valid_multidocument_unions() {
    assert_eq!(validate_unions(&[], &[]), Ok(()));
    let ontology = [
        "<urn:subject> <urn:p> <urn:value> .".into(),
        format!("<urn:subject> <{RDF}type> <urn:Class> ."),
    ];
    let shapes = [format!("<urn:shape> <{SH}targetClass> <urn:Class> ; <{SH}property> _:b . _:b <{SH}path> <urn:p> ; <{SH}minCount> 1 .")];
    assert_eq!(validate_unions(&ontology, &shapes), Ok(()));
}

#[test]
fn malformed_files_preserve_existing_refusals() {
    assert_eq!(
        validate_unions(&["bad".into()], &[]),
        Err((
            PackViolationCode::OntologyInvalid,
            "an ontology file is not valid Turtle"
        ))
    );
    assert_eq!(
        validate_unions(&[], &["bad".into()]),
        Err((
            PackViolationCode::ShapesInvalid,
            "a shapes file is not valid Turtle"
        ))
    );
    assert_eq!(
        code(validate_unions(&["bad".into()], &["bad".into()])),
        PackViolationCode::OntologyInvalid
    );
}

fn disjoint_classes() -> String {
    format!("<urn:A> <{RDF}type> <{OWL}Class> ; <{OWL}disjointWith> <urn:B> . <urn:B> <{RDF}type> <{OWL}Class> .")
}

#[test]
fn class_and_abox_contradictions_use_the_engine_reasoner() {
    for contradiction in [
        "<urn:A> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:B> .".into(),
        format!("<urn:x> <{RDF}type> <urn:A>, <urn:B> ."),
    ] {
        assert_eq!(
            code(validate_unions(&[disjoint_classes(), contradiction], &[])),
            PackViolationCode::OntologyInconsistent
        );
    }
    assert_eq!(validate_unions(&[disjoint_classes()], &[]), Ok(()));
}

#[test]
fn blank_shapes_are_isolated_across_files() {
    let ontology = ["<urn:x> <urn:p> <urn:o> .".into()];
    let shapes = [
        format!("_:b <{SH}targetNode> <urn:x> ; <{SH}path> <urn:p> ; <{SH}minCount> 1 ."),
        format!("_:b <{SH}targetNode> <urn:y> ; <{SH}path> <urn:p> ; <{SH}maxCount> 0 ."),
    ];
    assert_eq!(validate_unions(&ontology, &shapes), Ok(()));
}

#[test]
fn unsupported_paths_and_service_keep_the_existing_refusals() {
    let path = [format!("<urn:s> <{SH}path> _:b .")];
    assert_eq!(
        validate_unions(&[], &path),
        Err((
            PackViolationCode::ShapesInvalid,
            "SHACL property paths must be predicate IRIs"
        ))
    );
    assert_eq!(
        validate_unions(&[], &["# SERVICE".into()]),
        Err((
            PackViolationCode::ShapesInvalid,
            "SHACL SPARQL SERVICE constraints are forbidden"
        ))
    );
}

#[test]
fn missing_required_property_is_a_shacl_violation() {
    let shapes = [format!(
        "<urn:s> <{SH}targetNode> <urn:x> ; <{SH}path> <urn:p> ; <{SH}minCount> 1 ."
    )];
    assert_eq!(
        code(validate_unions(&[], &shapes)),
        PackViolationCode::ShaclViolation
    );
}

#[test]
fn aggregate_limits_are_checked_before_any_syntax() {
    assert_eq!(
        code(validate_unions(&vec!["bad".into(); MAX_DOCUMENTS + 1], &[])),
        PackViolationCode::PackTooLarge
    );
    assert_eq!(
        code(validate_unions(
            &["bad".repeat(MAX_DOCUMENT_BYTES / 3 + 1)],
            &[]
        )),
        PackViolationCode::PackTooLarge
    );
    let mut documents = vec![" ".repeat(MAX_DOCUMENT_BYTES); MAX_INPUT_BYTES / MAX_DOCUMENT_BYTES];
    assert_eq!(limits::preflight(&documents, &[]), Ok(()));
    documents.push("bad".into());
    assert_eq!(
        code(validate_unions(&documents, &[])),
        PackViolationCode::PackTooLarge
    );
    assert_eq!(
        limits::preflight(&vec![String::new(); MAX_DOCUMENTS], &[]),
        Ok(())
    );
}

#[test]
fn union_triple_limit_is_aggregate_and_streamed() {
    let documents = vec!["<s:s><p:p><o:o>.".repeat(50_001); 2];
    assert_eq!(
        validate_unions(&documents, &[]),
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "ontology union exceeds the triple validation budget"
        ))
    );
    assert_eq!(
        validate_unions(&[], &documents),
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "shapes union exceeds the triple validation budget"
        ))
    );
}

#[test]
fn original_shacl_product_budget_is_preserved() {
    let ontology = ["<s:s><p:p><o:o>.".repeat(101)];
    let shapes = ["<s:s><p:p><o:o>.".repeat(100_000)];
    assert_eq!(
        validate_unions(&ontology, &shapes),
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "SHACL validation exceeds the deterministic evaluation budget"
        ))
    );
}

#[test]
fn reasoning_exhaustion_uses_the_fixed_ten_million_budget() {
    assert_eq!(REASONING_STEPS, 10_000_000);
    let chain: String = (0..4_500)
        .map(|i| {
            format!(
                "<urn:C{i}> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <urn:C{}> .\n",
                i + 1
            )
        })
        .collect();
    assert_eq!(
        validate_unions(&[chain], &[]),
        Err((
            PackViolationCode::ValidationBudgetExceeded,
            "ontology reasoning exceeds the deterministic step budget"
        ))
    );
}

#[test]
fn shared_admission_preserves_engine_resource_refusals() {
    let mut deep = "<urn:S0> <http://www.w3.org/ns/shacl#targetNode> <urn:x> .\n".to_string();
    let mut branching = deep.clone();
    for level in 0..45 {
        deep.push_str(&format!(
            "<urn:S{level}> <http://www.w3.org/ns/shacl#node> <urn:S{}> .\n",
            level + 1
        ));
    }
    for level in 0..18 {
        let next = level + 1;
        branching.push_str(&format!(
            "<urn:S{level}> <http://www.w3.org/ns/shacl#and> ( <urn:S{next}> <urn:S{next}> ) .\n"
        ));
    }
    for (document, expected) in [
        (deep, eg_shacl::DEPTH_EXCEEDED),
        (branching, eg_shacl::WORK_EXCEEDED),
    ] {
        let engine_error = eg_shacl::validate_icv_turtle(&document, "").unwrap_err();
        assert_eq!(engine_error, expected);
        assert!(eg_shacl::is_resource_refusal(&engine_error));
        assert_eq!(
            validate_unions(&[], &[document]).unwrap_err().0,
            PackViolationCode::ValidationBudgetExceeded
        );
    }
}

#[test]
fn shared_admission_matches_engine_semantic_and_annotation_outcomes() {
    let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <urn:ex:> . ";
    let data = "<urn:ex:x> <urn:ex:p> <urn:ex:y> .";
    for (constraint, verdict) in [
        (
            "sh:path ex:p ; sh:equals ex:q",
            Some(PackViolationCode::ShapesInvalid),
        ),
        (
            "sh:path ( ex:p ex:q )",
            Some(PackViolationCode::ShapesInvalid),
        ),
        ("sh:in ()", Some(PackViolationCode::ShaclViolation)),
        (
            "sh:name \"label\" ; sh:description \"metadata\" ; ex:annotation ex:Value",
            None,
        ),
    ] {
        let shape = format!("{prefix} ex:S sh:targetNode ex:x ; {constraint} .");
        let engine = eg_shacl::validate_icv_turtle(&shape, data);
        let direct = match engine {
            Err(_) => Some(PackViolationCode::ShapesInvalid),
            Ok(report) if !report.conforms => Some(PackViolationCode::ShaclViolation),
            Ok(_) => None,
        };
        assert_eq!(direct, verdict, "engine: {constraint}");
        assert_eq!(
            validate_unions(&[data.into()], &[shape]).err().map(|e| e.0),
            verdict,
            "shared admission: {constraint}"
        );
    }
}
