//! Native parser contract for the shared data-platform vocabulary.

use eg_core::graph::{current_core_set_digest, GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};
use eg_shacl::validate_turtle;

const SHAPES: &str = include_str!("../ontology/data_platform-v1.shapes.ttl");

const DP: &str = "http://knuckles.team/kg/data-platform#";
const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const SHACL: &str = "http://www.w3.org/ns/shacl#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

// spec: EG-DATAPLAT-VOCAB-R001.1
#[test]
fn shared_terms_present() {
    let triples = parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses");

    assert!(has_iri(
        &triples,
        &format!("{DP}DataStore"),
        &format!("{RDF}type"),
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}storeFamily"),
        &format!("{RDF}type"),
        &format!("{OWL}DatatypeProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}inStore"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}viaConnection"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}asOf"),
        &format!("{RDF}type"),
        &format!("{OWL}DatatypeProperty")
    ));

    assert!(has_iri(
        &triples,
        &format!("{DP}storeFamily"),
        &format!("{RDFS}domain"),
        &format!("{DP}DataStore")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}storeFamily"),
        &format!("{RDFS}range"),
        &format!("{XSD}string")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}inStore"),
        &format!("{RDFS}range"),
        &format!("{DP}DataStore")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}viaConnection"),
        &format!("{RDFS}domain"),
        &format!("{DP}DataStore")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}viaConnection"),
        &format!("{RDFS}range"),
        &format!("{VG}SourceConnection")
    ));
    assert!(has_iri(
        &triples,
        &format!("{DP}asOf"),
        &format!("{RDFS}range"),
        &format!("{XSD}dateTime")
    ));
}

// spec: EG-DATAPLAT-VOCAB-R001.2
#[test]
fn store_mapping_subclass_of_virtual_mapping() {
    let triples = parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses");

    assert!(has_iri(
        &triples,
        &format!("{DP}StoreMapping"),
        &format!("{RDFS}subClassOf"),
        &format!("{VG}VirtualMapping")
    ));
    for property in [
        "contractKind",
        "viaOperation",
        "accelerationMode",
        "materialisationReason",
    ] {
        let property_iri = format!("{DP}{property}");
        assert!(
            has_iri(
                &triples,
                &property_iri,
                &format!("{RDF}type"),
                &format!("{OWL}DatatypeProperty")
            ),
            "{property} is a datatype property"
        );
        assert!(
            has_iri(
                &triples,
                &property_iri,
                &format!("{RDFS}domain"),
                &format!("{DP}StoreMapping")
            ),
            "{property} is owned by StoreMapping"
        );
        assert!(
            has_iri(
                &triples,
                &property_iri,
                &format!("{RDFS}range"),
                &format!("{XSD}string")
            ),
            "{property} has the string range"
        );
    }
}

// spec: EG-DATAPLAT-VOCAB-R001.4
#[test]
fn core_source_registered() {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    let source = sources
        .core
        .get("core:data-platform@1")
        .expect("the data-platform vocabulary is registered");
    let ontology = source
        .ontology_ttl
        .as_deref()
        .expect("the data-platform source carries an ontology");
    let triples = parse_turtle(ontology).expect("the registered ontology parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/data-platform",
        &format!("{RDF}type"),
        &format!("{OWL}Ontology")
    ));

    match &source.origin {
        SchemaSourceOrigin::Core {
            module,
            version,
            set_digest,
        } => {
            assert_eq!(module, "data-platform");
            assert_eq!(*version, 1);
            assert_eq!(*set_digest, current_core_set_digest());
        }
        other => panic!("unexpected source origin: {other:?}"),
    }

    let catalog = sources
        .core
        .get("core:catalog@1")
        .and_then(|source| source.ontology_ttl.as_deref())
        .expect("the aggregate catalog carries an ontology");
    assert!(catalog.contains("<http://knuckles.team/kg/data-platform>"));
}

fn assert_typed(triples: &[Triple], local: &str, kind: &str) {
    assert!(
        has_iri(
            triples,
            &format!("{DP}{local}"),
            &format!("{RDF}type"),
            &format!("{OWL}{kind}")
        ),
        "{local} is an owl:{kind}"
    );
}

fn assert_domain_range(triples: &[Triple], local: &str, domain: &str, range: &str) {
    assert!(
        has_iri(
            triples,
            &format!("{DP}{local}"),
            &format!("{RDFS}domain"),
            &format!("{DP}{domain}")
        ),
        "{local} domain is {domain}"
    );
    let range_iri = match range.strip_prefix("xsd:") {
        Some(local_name) => format!("{XSD}{local_name}"),
        None => format!("{DP}{range}"),
    };
    assert!(
        has_iri(
            triples,
            &format!("{DP}{local}"),
            &format!("{RDFS}range"),
            &range_iri
        ),
        "{local} range is {range}"
    );
}

fn load_data_platform() -> Vec<Triple> {
    parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses")
}

// spec: EG-DATAPLAT-VOCAB-R002.1
#[test]
fn relational_classes_present() {
    let triples = load_data_platform();
    for class in [
        "Database",
        "Schema",
        "Table",
        "View",
        "Column",
        "ForeignKey",
    ] {
        assert_typed(&triples, class, "Class");
    }
}

// spec: EG-DATAPLAT-VOCAB-R002.2
#[test]
fn relational_properties_domain_range() {
    let triples = load_data_platform();
    assert_domain_range(&triples, "inSchema", "Table", "Schema");
    assert_domain_range(&triples, "hasColumn", "Table", "Column");
    assert_domain_range(&triples, "referencesTable", "ForeignKey", "Table");
    assert_domain_range(&triples, "isPrimaryKey", "Column", "xsd:boolean");
    assert_domain_range(&triples, "sqlDialect", "Database", "xsd:string");
}

// spec: EG-DATAPLAT-VOCAB-R002.3
#[test]
fn document_terms_present() {
    let triples = load_data_platform();
    assert_typed(&triples, "Collection", "Class");
    assert_typed(&triples, "DocumentField", "Class");
    assert_typed(&triples, "fieldPath", "DatatypeProperty");
    assert_typed(&triples, "fieldType", "DatatypeProperty");
    assert_typed(&triples, "presenceRatio", "DatatypeProperty");
    assert_typed(&triples, "documentCount", "DatatypeProperty");
}

// spec: EG-DATAPLAT-VOCAB-R003.1
#[test]
fn object_terms_present_without_content_property() {
    let triples = load_data_platform();
    assert_typed(&triples, "Bucket", "Class");
    assert_typed(&triples, "StoredObject", "Class");
    for property in ["objectKey", "byteSize", "lastModified"] {
        assert_typed(&triples, property, "DatatypeProperty");
    }
    for forbidden in ["content", "body", "payload", "data"] {
        assert!(
            !has_iri(
                &triples,
                &format!("{DP}{forbidden}"),
                &format!("{RDF}type"),
                &format!("{OWL}DatatypeProperty")
            ),
            "{forbidden} must not be a property of stored objects"
        );
    }
}

// spec: EG-DATAPLAT-VOCAB-R003.2
#[test]
fn vector_terms_present() {
    let triples = load_data_platform();
    assert_typed(&triples, "VectorCollection", "Class");
    assert_typed(&triples, "EmbeddingSpace", "Class");
    for property in [
        "embeddingModel",
        "dimensions",
        "distanceMetric",
        "vectorCount",
    ] {
        assert_typed(&triples, property, "DatatypeProperty");
    }
    assert_typed(&triples, "usesEmbeddingSpace", "ObjectProperty");
}

// spec: EG-DATAPLAT-VOCAB-R004.1
#[test]
fn stream_terms_present() {
    let triples = load_data_platform();
    for class in ["Topic", "Partition", "ConsumerGroup", "PartitionLag"] {
        assert_typed(&triples, class, "Class");
    }
    assert_typed(&triples, "partitionOf", "ObjectProperty");
    assert_typed(&triples, "subscribesTo", "ObjectProperty");
    assert_typed(&triples, "lag", "DatatypeProperty");
    assert_typed(&triples, "groupState", "DatatypeProperty");
}

// spec: EG-DATAPLAT-VOCAB-R004.3
#[test]
fn analytics_terms_present() {
    let triples = load_data_platform();
    assert_typed(&triples, "AnalyticsSite", "Class");
    assert_typed(&triples, "BehaviorMetric", "Class");
    for property in [
        "metricName",
        "metricValue",
        "windowDays",
        "dimensionName",
        "dimensionValue",
    ] {
        assert_typed(&triples, property, "DatatypeProperty");
    }
}

// spec: EG-DATAPLAT-VOCAB-R004.4
#[test]
fn ml_terms_present() {
    let triples = load_data_platform();
    assert!(has_iri(
        &triples,
        &format!("{DP}Dataset"),
        &format!("{RDFS}subClassOf"),
        "http://knuckles.team/kg#Dataset"
    ));
    assert_typed(&triples, "Model", "Class");
    assert!(
        !has_iri(
            &triples,
            &format!("{DP}Model"),
            &format!("{OWL}equivalentClass"),
            "http://knuckles.team/kg#Model"
        ),
        "dp:Model must not be equivalent to the narrower core Model"
    );
    assert_typed(&triples, "TrainingRun", "Class");
    assert_typed(&triples, "EvaluationResult", "Class");
    assert_typed(&triples, "trainedOn", "ObjectProperty");
    assert_typed(&triples, "producedModel", "ObjectProperty");
    assert_typed(&triples, "evaluatedOn", "ObjectProperty");
    assert_typed(&triples, "baselineRun", "ObjectProperty");
}

fn subject_is(triple: &Triple, subject: &str) -> bool {
    match &triple.subject {
        NamedOrBlankNode::NamedNode(node) => node.as_str() == subject,
        NamedOrBlankNode::BlankNode(node) => format!("_:{}", node.as_str()) == subject,
    }
}

fn predicate_is(triple: &Triple, predicate: &str) -> bool {
    triple.predicate.as_str() == predicate
}

/// Object values of `subject predicate ?object` as strings (IRIs and literals).
fn object_values(triples: &[Triple], subject: &str, predicate: &str) -> Vec<String> {
    triples
        .iter()
        .filter(|triple| subject_is(triple, subject) && predicate_is(triple, predicate))
        .filter_map(|triple| match &triple.object {
            Term::NamedNode(node) => Some(node.as_str().to_string()),
            Term::Literal(literal) => Some(literal.value().to_string()),
            other => match other {
                Term::BlankNode(node) => Some(format!("_:{}", node.as_str())),
                // RDF-star quoted triples are not object values for these assertions.
                _ => None,
            },
        })
        .collect()
}

fn is_class(triples: &[Triple], iri: &str) -> bool {
    has_iri(triples, iri, &format!("{RDF}type"), &format!("{OWL}Class"))
}

fn has_domain_range(triples: &[Triple], iri: &str, domain: Option<&str>, range: &str) {
    if let Some(domain) = domain {
        assert!(
            has_iri(triples, iri, &format!("{RDFS}domain"), domain),
            "{iri} has the declared domain"
        );
    }
    assert!(
        has_iri(triples, iri, &format!("{RDFS}range"), range),
        "{iri} has the declared range"
    );
}

// spec: EG-DATAPLAT-VOCAB-R005.1
#[test]
fn secret_metadata_terms_present() {
    let triples = parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses");

    for class in [
        "SecretEngine",
        "SecretPath",
        "AccessPolicy",
        "RotationState",
        "PolicyPathGrant",
    ] {
        assert!(
            is_class(&triples, &format!("{DP}{class}")),
            "{class} is declared"
        );
    }
    for property in ["hasRotationState", "grantPolicy", "grantPath"] {
        assert!(
            has_iri(
                &triples,
                &format!("{DP}{property}"),
                &format!("{RDF}type"),
                &format!("{OWL}ObjectProperty")
            ),
            "{property} is declared"
        );
    }
    for property in [
        "lastRotatedAt",
        "currentVersion",
        "pathName",
        "grantsCapability",
        "leaseExpiresAt",
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{DP}{property}"),
                &format!("{RDF}type"),
                &format!("{OWL}DatatypeProperty")
            ),
            "{property} is declared"
        );
    }
    has_domain_range(
        &triples,
        &format!("{DP}hasRotationState"),
        Some(&format!("{DP}SecretPath")),
        &format!("{DP}RotationState"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}lastRotatedAt"),
        Some(&format!("{DP}RotationState")),
        &format!("{XSD}dateTime"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}currentVersion"),
        Some(&format!("{DP}RotationState")),
        &format!("{XSD}integer"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}pathName"),
        None,
        &format!("{XSD}string"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}grantPolicy"),
        Some(&format!("{DP}PolicyPathGrant")),
        &format!("{DP}AccessPolicy"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}grantPath"),
        Some(&format!("{DP}PolicyPathGrant")),
        &format!("{DP}SecretPath"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}grantsCapability"),
        Some(&format!("{DP}PolicyPathGrant")),
        &format!("{XSD}string"),
    );
    has_domain_range(
        &triples,
        &format!("{DP}leaseExpiresAt"),
        None,
        &format!("{XSD}dateTime"),
    );
}

/// Whether `shape` is `sh:closed` and which property IRIs its `sh:property`
/// list allows. A closed shape rejects every property outside that list, so a
/// value/token triple on the class is the violation itself.
fn closed_shape(triples: &[Triple], shape: &str) -> (bool, Vec<String>) {
    let closed_values = object_values(triples, shape, &format!("{SHACL}closed"));
    let closed = closed_values.iter().any(|value| value == "true");
    let property = format!("{SHACL}property");
    let path = format!("{SHACL}path");
    let mut allowed = Vec::new();
    for triple in triples.iter() {
        if subject_is(triple, shape) && predicate_is(triple, &property) {
            if let Term::BlankNode(node) = &triple.object {
                let node = format!("_:{}", node.as_str());
                for path_triple in triples.iter() {
                    if subject_is(path_triple, &node) && predicate_is(path_triple, &path) {
                        if let Term::NamedNode(property) = &path_triple.object {
                            allowed.push(property.as_str().to_string());
                        }
                    }
                }
            }
        }
    }
    (closed, allowed)
}

// spec: EG-DATAPLAT-VOCAB-R005.2
#[test]
fn secret_path_with_extra_property_rejected() {
    let shapes = parse_turtle(SHAPES).expect("the data-platform shapes parse");

    let (closed, allowed) = closed_shape(&shapes, &format!("{DP}SecretPathShape"));
    assert!(closed, "the SecretPath shape is sh:closed");
    // The allowed metadata list is exactly the path and rotation metadata plus
    // the shared store/provenance links; anything else is the violation.
    for property in [
        "pathName",
        "hasRotationState",
        "inStore",
        "viaConnection",
        "asOf",
    ] {
        assert!(
            allowed.contains(&format!("{DP}{property}")),
            "{property} is on the allowed metadata list"
        );
    }
    // A `value` (or `token`) triple on a dp:SecretPath is outside the closed
    // list, so the closed shape rejects it.
    for extra in [
        "value",
        "token",
        "password",
        "ciphertext",
        "rules",
        "data",
        "secret",
    ] {
        assert!(
            !allowed.contains(&format!("{DP}{extra}"))
                && !allowed
                    .iter()
                    .any(|iri| iri.ends_with(&format!("#{extra}"))),
            "{extra} is not an allowed property of a secret path"
        );
    }

    // The other four closed shapes are present and closed too.
    for shape in [
        "SecretEngineShape",
        "AccessPolicyShape",
        "RotationStateShape",
        "PolicyPathGrantShape",
    ] {
        let (closed, _) = closed_shape(&shapes, &format!("{DP}{shape}"));
        assert!(closed, "the {shape} shape is sh:closed");
    }
}

// spec: EG-DATAPLAT-VOCAB-R005.3
#[test]
fn secret_value_refused_and_no_value_property() {
    let vocabulary = parse_turtle(include_str!("../ontology/data_platform-v1.ttl"))
        .expect("the data-platform vocabulary parses");
    let shapes = parse_turtle(SHAPES).expect("the data-platform shapes parse");

    // A dp:SecretPath carrying a value (or token) triple is rejected by the
    // closed shape: the property is outside the allowed metadata list.
    let (closed, allowed) = closed_shape(&shapes, &format!("{DP}SecretPathShape"));
    assert!(closed, "the SecretPath shape is sh:closed");
    for extra in ["value", "token"] {
        assert!(
            !allowed.contains(&format!("{DP}{extra}")),
            "a SecretPath {extra} triple is not on the closed allow-list"
        );
    }

    // The vocabulary declares no property named value, secret, token, password,
    // ciphertext or rules.
    let datatype_property = format!("{OWL}DatatypeProperty");
    let object_property = format!("{OWL}ObjectProperty");
    for forbidden in [
        "value",
        "secret",
        "token",
        "password",
        "ciphertext",
        "rules",
    ] {
        let forbidden_iri = format!("{DP}{forbidden}");
        assert!(
            !vocabulary.iter().any(|triple| {
                subject_is(triple, &forbidden_iri)
                    && predicate_is(triple, &format!("{RDF}type"))
                    && matches!(
                        &triple.object,
                        Term::NamedNode(node)
                            if node.as_str() == datatype_property.as_str()
                                || node.as_str() == object_property.as_str()
                    )
            }),
            "no property named {forbidden} is declared"
        );
    }
}

/// Parse a StoreMapping fixture and assert each mapping row is well-formed:
/// a dp:StoreMapping with a closed contractKind, a viaOperation for every
/// mcp-operation row, a closed accelerationMode, and a materialisation reason
/// exactly where the row is materialised.
fn is_store_mapping_subject(triple: &Triple) -> Option<String> {
    let mapping_type = format!("{DP}StoreMapping");
    if predicate_is(triple, &format!("{RDF}type"))
        && matches!(
            &triple.object,
            Term::NamedNode(node) if node.as_str() == mapping_type.as_str()
        )
    {
        match &triple.subject {
            NamedOrBlankNode::NamedNode(node) => Some(node.as_str().to_string()),
            _ => None,
        }
    } else {
        None
    }
}

fn store_mapping_fixture(fixture: &str, expected_operations: &[&str]) {
    let triples = parse_turtle(fixture).expect("the mapping fixture parses");

    let mappings: Vec<String> = triples
        .iter()
        .filter_map(is_store_mapping_subject)
        .collect();
    assert!(
        !mappings.is_empty(),
        "the fixture declares at least one StoreMapping"
    );

    let contract_kinds = ["r2rml-sql", "document-shape", "mcp-operation"];
    let modes = ["virtual", "materialised"];
    for subject in &mappings {
        let kind = object_values(&triples, subject, &format!("{DP}contractKind"));
        assert!(
            !kind.is_empty() && contract_kinds.contains(&kind[0].as_str()),
            "contractKind is closed for {subject}"
        );
        let mode = object_values(&triples, subject, &format!("{DP}accelerationMode"));
        assert!(
            !mode.is_empty() && modes.contains(&mode[0].as_str()),
            "accelerationMode is virtual or materialised for {subject}"
        );
        if kind[0] == "mcp-operation" {
            let operation = object_values(&triples, subject, &format!("{DP}viaOperation"));
            assert!(
                !operation.is_empty() && !operation[0].is_empty(),
                "an mcp-operation mapping names its viaOperation: {subject}"
            );
        }
        if mode[0] == "materialised" {
            let reason = object_values(&triples, subject, &format!("{DP}materialisationReason"));
            assert!(
                !reason.is_empty() && !reason[0].trim().is_empty(),
                "a materialised mapping records its reason: {subject}"
            );
        }
    }

    let operations: Vec<String> = mappings
        .iter()
        .flat_map(|subject| object_values(&triples, subject, &format!("{DP}viaOperation")))
        .collect();
    for operation in expected_operations {
        assert!(
            operations.iter().any(|mapped| mapped == operation),
            "the fixture maps {operation}"
        );
    }
}

// spec: EG-DATAPLAT-VOCAB-R006.1
#[test]
fn mapping_fixture_sql_validates() {
    validate_fixture(include_str!("../ontology/fixtures/data_platform/sql.ttl"));
    store_mapping_fixture(
        include_str!("../ontology/fixtures/data_platform/sql.ttl"),
        &[
            "sql_schema:tables",
            "sql_schema:columns",
            "sql_schema:foreign_keys",
            "sql_schema:constraints",
        ],
    );
}

// spec: EG-DATAPLAT-VOCAB-R006.8
#[test]
fn mapping_fixture_data_science_validates() {
    validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/data-science.ttl"
    ));
    store_mapping_fixture(
        include_str!("../ontology/fixtures/data_platform/data-science.ttl"),
        &[
            "load_dataset",
            "dataset_lineage",
            "rank_models",
            "evaluate_model",
            "fit_model:fit",
        ],
    );
}

const DP_SHAPES: &str = SHAPES;
const VG_SHAPES: &str = include_str!("../ontology/virtual_graph-v1.shapes.ttl");

/// One minimal complete store-mapping document with the given `dp:viaOperation`:
/// a metadata contract, one approved `dp:StoreMapping` onto `dp:SecretPath` and
/// its one predicate-field pair.
fn store_mapping_document(via_operation: &str) -> String {
    format!(
        r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix vg: <http://knuckles.team/kg/virtual-graph#> .
@prefix ex: <http://example.org/data-platform/> .

ex:contract a vg:MetadataContract ;
    vg:contractSource <http://example.org/data-platform/connection> ;
    vg:schemaVersion "1" ;
    vg:contentDigest "sha256:b8cc56e85870f9307795bd43189dc66c516c662bd1df459a76537c4f1e7f9c44" ;
    vg:discoveredEntity "logical_paths" ;
    vg:discoveredField "path" .

ex:mapping a dp:StoreMapping ;
    vg:mapsContract ex:contract ;
    vg:mapsOntologyClass dp:SecretPath ;
    vg:mapsEntity "logical_paths" ;
    vg:keyField "path" ;
    vg:hasPredicateFieldPair [
        a vg:PredicateFieldPair ;
        vg:predicate dp:pathName ;
        vg:field "path"
    ] ;
    vg:approved true ;
    dp:contractKind "mcp-operation" ;
    dp:viaOperation "{via_operation}" ;
    dp:accelerationMode "virtual" .
"#
    )
}

/// The literal value of a term, if it is a literal.
fn literal_value(term: &eg_rdf::oxrdf::Term) -> Option<&str> {
    match term.as_ref() {
        eg_rdf::oxrdf::TermRef::Literal(literal) => Some(literal.value()),
        _ => None,
    }
}

/// The IRI of a named term, without Turtle's angle brackets.
fn iri_of(term: &eg_rdf::oxrdf::Term) -> Option<String> {
    if term.is_named_node() {
        let rendered = term.to_string();
        return Some(rendered[1..rendered.len() - 1].to_string());
    }
    None
}

/// The IRI of a triple subject, without Turtle's angle brackets.
fn subject_iri_of(subject: &eg_rdf::oxrdf::NamedOrBlankNode) -> String {
    let rendered = subject.to_string();
    rendered[1..rendered.len() - 1].to_string()
}

/// Validates a mapping fixture against the data-platform and virtual-graph
/// shapes and returns its triples.
fn validate_fixture(document: &str) -> Vec<Triple> {
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), document)
        .expect("the mapping fixture parses before validation");
    assert!(
        report.conforms,
        "the mapping fixture must conform to the data-platform and virtual-graph shapes: {:?}",
        report.results
    );
    parse_turtle(document).expect("the mapping fixture parses")
}

/// Asserts that the named `dp:viaOperation` values each appear on some
/// `dp:StoreMapping` node of the fixture.
fn assert_via_operations(triples: &[Triple], operations: &[&str]) {
    let via_operation = NamedNode::new(format!("{DP}viaOperation")).unwrap();
    for operation in operations {
        assert!(
            triples.iter().any(|triple| {
                triple.predicate == via_operation
                    && triple.object
                        == eg_rdf::oxrdf::Term::Literal(eg_rdf::oxrdf::Literal::new_simple_literal(
                            *operation,
                        ))
            }),
            "a mapping via {operation} must be present in the fixture"
        );
    }
}
// spec: EG-DATAPLAT-VOCAB-R002.4
#[test]
fn document_field_shape_rejects_ratio_2() {
    // A presenceRatio of 2 is outside [0,1] and must be refused.
    let bad_ratio = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:field a dp:DocumentField ;
    dp:fieldPath "address.city" ;
    dp:fieldType "string" ;
    dp:presenceRatio 2.0 .
"#;
    let report = validate_turtle(DP_SHAPES, bad_ratio)
        .expect("the ratio-2 document parses before validation");
    assert!(
        !report.conforms,
        "a presenceRatio of 2 must be rejected: {:?}",
        report.results
    );

    // A DocumentFieldType member (including mixed) with a ratio inside [0,1] conforms.
    let good = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:field a dp:DocumentField ;
    dp:fieldPath "address.city" ;
    dp:fieldType "mixed" ;
    dp:presenceRatio 0.5 .
"#;
    let report =
        validate_turtle(DP_SHAPES, good).expect("the conforming document parses before validation");
    assert!(
        report.conforms,
        "a DocumentFieldType member with a ratio in [0,1] must conform: {:?}",
        report.results
    );

    // A fieldType outside the DocumentFieldType list is refused on its own.
    let bad_type = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:field a dp:DocumentField ;
    dp:fieldPath "address.city" ;
    dp:fieldType "weird" ;
    dp:presenceRatio 0.5 .
"#;
    let report = validate_turtle(DP_SHAPES, bad_type)
        .expect("the bad-type document parses before validation");
    assert!(
        !report.conforms,
        "fieldType must be a DocumentFieldType member: {:?}",
        report.results
    );
}

// spec: EG-DATAPLAT-VOCAB-R003.3
#[test]
fn vector_dimension_zero_rejected() {
    // A zero-embedding-space (dimensions 0) is refused: dimensions is a
    // positive integer.
    let zero_dimensions = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:collection a dp:VectorCollection ;
    dp:usesEmbeddingSpace ex:space .

ex:space a dp:EmbeddingSpace ;
    dp:embeddingModel "all-MiniLM-L6-v2" ;
    dp:dimensions 0 .
"#;
    let report = validate_turtle(DP_SHAPES, zero_dimensions)
        .expect("the zero-dimension document parses before validation");
    assert!(
        !report.conforms,
        "a zero-dimensional embedding space must be rejected: {:?}",
        report.results
    );

    // A collection without any embedding space is refused too.
    let no_space = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:collection a dp:VectorCollection ;
    dp:vectorCount 1000 .
"#;
    let report = validate_turtle(DP_SHAPES, no_space)
        .expect("the missing-space document parses before validation");
    assert!(
        !report.conforms,
        "a vector collection without an embedding space must be rejected: {:?}",
        report.results
    );

    // A collection with exactly one positive-dimensional embedding space conforms.
    let good = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:collection a dp:VectorCollection ;
    dp:usesEmbeddingSpace ex:space .

ex:space a dp:EmbeddingSpace ;
    dp:embeddingModel "all-MiniLM-L6-v2" ;
    dp:dimensions 1536 ;
    dp:distanceMetric "cosine" .
"#;
    let report =
        validate_turtle(DP_SHAPES, good).expect("the conforming document parses before validation");
    assert!(
        report.conforms,
        "one positive-dimensional embedding space must conform: {:?}",
        report.results
    );
}

// spec: EG-DATAPLAT-VOCAB-R004.2
#[test]
fn negative_lag_rejected() {
    // A negative lag is refused: lag is an integer >= 0.
    let negative = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/data-platform/> .

ex:lag a dp:PartitionLag ;
    dp:lag -3 ;
    dp:asOf "2026-10-28T00:00:00Z"^^xsd:dateTime .
"#;
    let report = validate_turtle(DP_SHAPES, negative)
        .expect("the negative-lag document parses before validation");
    assert!(
        !report.conforms,
        "a negative lag must be rejected: {:?}",
        report.results
    );

    // A lag without its observation time is refused too.
    let no_asof = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:lag a dp:PartitionLag ;
    dp:lag 42 .
"#;
    let report = validate_turtle(DP_SHAPES, no_asof)
        .expect("the missing-asOf document parses before validation");
    assert!(
        !report.conforms,
        "a partition lag without asOf must be rejected: {:?}",
        report.results
    );

    // A zero lag with an observation time conforms (boundary of [0, +inf)).
    let good = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/data-platform/> .

ex:lag a dp:PartitionLag ;
    dp:lag 0 ;
    dp:asOf "2026-10-28T00:00:00Z"^^xsd:dateTime .
"#;
    let report =
        validate_turtle(DP_SHAPES, good).expect("the conforming document parses before validation");
    assert!(
        report.conforms,
        "a zero lag with asOf must conform: {:?}",
        report.results
    );
}
// spec: EG-DATAPLAT-VOCAB-R005.4
#[test]
fn openbao_value_operation_mapping_refused() {
    // Every secret-value operation is refused by the StoreMapping denylist.
    for operation in [
        "openbao_mcp_kv:kv2_get",
        "openbao_mcp_kv:kv1_get",
        "openbao_mcp_logical:read",
        "openbao_mcp_logical:unwrap",
    ] {
        let document = store_mapping_document(operation);
        let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &document)
            .expect("the refused mapping parses before validation");
        assert!(
            !report.conforms,
            "a StoreMapping via {operation} must be refused: {:?}",
            report.results
        );
    }

    // A metadata-only openbao operation is not on the denylist: the same
    // mapping with it conforms.
    let good = store_mapping_document("openbao_mcp_logical:list");
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &good)
        .expect("the metadata mapping parses before validation");
    assert!(
        report.conforms,
        "a metadata-only viaOperation must conform: {:?}",
        report.results
    );
}

// spec: EG-DATAPLAT-VOCAB-R005.5
#[test]
fn grants_capability_closed_list() {
    // One policy granting read on path A and deny on path B: the permissions
    // stay scoped to their policy/path pair and conform.
    let scoped_grants = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:engine a dp:SecretEngine .

ex:policy a dp:AccessPolicy ;
    dp:inStore ex:engine .

ex:pathA a dp:SecretPath ;
    dp:pathName "secret/data/production/db" ;
    dp:inStore ex:engine .

ex:pathB a dp:SecretPath ;
    dp:pathName "secret/data/production/api" ;
    dp:inStore ex:engine .

ex:grantA a dp:PolicyPathGrant ;
    dp:grantPolicy ex:policy ;
    dp:grantPath ex:pathA ;
    dp:grantsCapability "read" ;
    dp:inStore ex:engine .

ex:grantB a dp:PolicyPathGrant ;
    dp:grantPolicy ex:policy ;
    dp:grantPath ex:pathB ;
    dp:grantsCapability "deny" ;
    dp:inStore ex:engine .
"#;
    let report = validate_turtle(DP_SHAPES, scoped_grants)
        .expect("the scoped-grant document parses before validation");
    assert!(
        report.conforms,
        "one policy granting read on path A and deny on path B must conform: {:?}",
        report.results
    );

    // A capability outside the closed list is refused.
    let bad_capability = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:engine a dp:SecretEngine .

ex:policy a dp:AccessPolicy ;
    dp:inStore ex:engine .

ex:pathA a dp:SecretPath ;
    dp:pathName "secret/data/production/db" ;
    dp:inStore ex:engine .

ex:grant a dp:PolicyPathGrant ;
    dp:grantPolicy ex:policy ;
    dp:grantPath ex:pathA ;
    dp:grantsCapability "admin" ;
    dp:inStore ex:engine .
"#;
    let report = validate_turtle(DP_SHAPES, bad_capability)
        .expect("the bad-capability document parses before validation");
    assert!(
        !report.conforms,
        "a grantsCapability outside the closed list must be rejected: {:?}",
        report.results
    );
}
// spec: EG-DATAPLAT-VOCAB-R006.2
#[test]
fn mapping_fixture_documentdb_validates() {
    let triples = validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/documentdb.ttl"
    ));
    assert_via_operations(
        &triples,
        &[
            "documentdb_collections:list_collections",
            "documentdb_analysis:aggregate",
        ],
    );
}

// spec: EG-DATAPLAT-VOCAB-R006.3
#[test]
fn mapping_fixture_objectstore_validates() {
    let triples = validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/objectstore.ttl"
    ));
    assert_via_operations(&triples, &["buckets:list", "objects:list", "objects:head"]);
}

// spec: EG-DATAPLAT-VOCAB-R006.4
#[test]
fn mapping_fixture_vector_validates() {
    let triples = validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/vector.ttl"
    ));
    assert_via_operations(&triples, &["vector_collection_management:list_collections"]);
}

// spec: EG-DATAPLAT-VOCAB-R006.5
#[test]
fn mapping_fixture_kafka_validates() {
    let triples = validate_fixture(include_str!("../ontology/fixtures/data_platform/kafka.ttl"));
    assert_via_operations(
        &triples,
        &[
            "kafka_topics:list",
            "kafka_partitions:list",
            "kafka_groups:lags",
            "kafka_groups:lag_summary",
        ],
    );
}

// spec: EG-DATAPLAT-VOCAB-R006.6
#[test]
fn mapping_fixture_openbao_validates() {
    let triples = validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/openbao.ttl"
    ));
    assert_via_operations(
        &triples,
        &["openbao_mcp_sys:get_mounts", "openbao_mcp_logical:list"],
    );
}

// spec: EG-DATAPLAT-VOCAB-R006.7
#[test]
fn mapping_fixture_clarity_validates() {
    let triples = validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/clarity.ttl"
    ));
    assert_via_operations(&triples, &["clarity_insights:get_data_export"]);
}
// spec: EG-DATAPLAT-VOCAB-R007.1
#[test]
fn materialised_without_reason_rejected() {
    fn mapping_with_mode(mode: &str, reason: Option<&str>) -> String {
        format!(
            r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix vg: <http://knuckles.team/kg/virtual-graph#> .
@prefix ex: <http://example.org/data-platform/> .

ex:contract a vg:MetadataContract ;
    vg:contractSource <http://example.org/data-platform/connection> ;
    vg:schemaVersion "1" ;
    vg:contentDigest "sha256:1076e71eb9f4543f53a06ccb20595f33afd5684c08ce0679f69ff9e0be1255d6" ;
    vg:discoveredEntity "logical_paths" ;
    vg:discoveredField "path" .

ex:mapping a dp:StoreMapping ;
    vg:mapsContract ex:contract ;
    vg:mapsOntologyClass dp:SecretPath ;
    vg:mapsEntity "logical_paths" ;
    vg:keyField "path" ;
    vg:hasPredicateFieldPair [
        a vg:PredicateFieldPair ;
        vg:predicate dp:pathName ;
        vg:field "path"
    ] ;
    vg:approved true ;
    dp:contractKind "mcp-operation" ;
    dp:viaOperation "openbao_mcp_logical:list" ;
    dp:accelerationMode "{mode}"{reason} .
"#,
            reason = reason
                .map(|r| format!(" ; dp:materialisationReason \"{r}\""))
                .unwrap_or_default()
        )
    }

    // A materialised mapping without a reason is a violation.
    let no_reason = mapping_with_mode("materialised", None);
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &no_reason)
        .expect("the no-reason mapping parses before validation");
    assert!(
        !report.conforms,
        "a materialised mapping without a materialisationReason must be rejected: {:?}",
        report.results
    );

    // The same mapping with a reason conforms.
    let with_reason = mapping_with_mode(
        "materialised",
        Some("Sustained lag needs history; recorded via the R038 AccelerationPolicy."),
    );
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &with_reason)
        .expect("the with-reason mapping parses before validation");
    assert!(
        report.conforms,
        "a materialised mapping with a reason must conform: {:?}",
        report.results
    );

    // A virtual mapping needs no reason.
    let virtual_mode = mapping_with_mode("virtual", None);
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &virtual_mode)
        .expect("the virtual mapping parses before validation");
    assert!(
        report.conforms,
        "a virtual mapping must conform without a reason: {:?}",
        report.results
    );
}

// spec: EG-DATAPLAT-VOCAB-R007.2
#[test]
fn mcp_operation_requires_via_operation() {
    fn mapping_with_kind(kind: &str, via_operation: Option<&str>) -> String {
        format!(
            r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix vg: <http://knuckles.team/kg/virtual-graph#> .
@prefix ex: <http://example.org/data-platform/> .

ex:contract a vg:MetadataContract ;
    vg:contractSource <http://example.org/data-platform/connection> ;
    vg:schemaVersion "1" ;
    vg:contentDigest "sha256:d780efe5a265c68dcfc72ea0ef8d07f04cabcae6e04cb2ce14d067e27987aca4" ;
    vg:discoveredEntity "logical_paths" ;
    vg:discoveredField "path" .

ex:mapping a dp:StoreMapping ;
    vg:mapsContract ex:contract ;
    vg:mapsOntologyClass dp:SecretPath ;
    vg:mapsEntity "logical_paths" ;
    vg:keyField "path" ;
    vg:hasPredicateFieldPair [
        a vg:PredicateFieldPair ;
        vg:predicate dp:pathName ;
        vg:field "path"
    ] ;
    vg:approved true ;
    dp:contractKind "{kind}"{operation} ;
    dp:accelerationMode "virtual" .
"#,
            operation = via_operation
                .map(|op| format!(" ; dp:viaOperation \"{op}\""))
                .unwrap_or_default()
        )
    }

    // An mcp-operation contract without its named operation is a violation.
    let missing_operation = mapping_with_kind("mcp-operation", None);
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &missing_operation)
        .expect("the missing-operation mapping parses before validation");
    assert!(
        !report.conforms,
        "an mcp-operation mapping without a viaOperation must be rejected: {:?}",
        report.results
    );

    // The same mapping with its named operation conforms.
    let named_operation = mapping_with_kind("mcp-operation", Some("openbao_mcp_logical:list"));
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &named_operation)
        .expect("the named-operation mapping parses before validation");
    assert!(
        report.conforms,
        "an mcp-operation mapping with a viaOperation must conform: {:?}",
        report.results
    );

    // Other contract kinds do not require a viaOperation.
    let r2rml = mapping_with_kind("r2rml-sql", None);
    let report = validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &r2rml)
        .expect("the r2rml mapping parses before validation");
    assert!(
        report.conforms,
        "an r2rml-sql mapping without a viaOperation must conform: {:?}",
        report.results
    );
}

// spec: EG-DATAPLAT-VOCAB-R007.3
#[test]
fn no_second_discovery_path() {
    let documents: &[&str] = &[
        include_str!("../ontology/data_platform-v1.ttl"),
        include_str!("../ontology/data_platform-v1.shapes.ttl"),
        include_str!("../ontology/fixtures/data_platform/sql.ttl"),
        include_str!("../ontology/fixtures/data_platform/documentdb.ttl"),
        include_str!("../ontology/fixtures/data_platform/objectstore.ttl"),
        include_str!("../ontology/fixtures/data_platform/vector.ttl"),
        include_str!("../ontology/fixtures/data_platform/kafka.ttl"),
        include_str!("../ontology/fixtures/data_platform/openbao.ttl"),
        include_str!("../ontology/fixtures/data_platform/clarity.ttl"),
        include_str!("../ontology/fixtures/data_platform/data-science.ttl"),
    ];

    // No dp: term or mapping fixture declares a discovery operation: a
    // vg:discoveredOperation triple does not exist anywhere — relational and
    // document entries come from the vg:MetadataContract only.
    let discovered_operation = NamedNode::new(format!("{VG}discoveredOperation")).unwrap();
    for document in documents {
        let triples = parse_turtle(document).expect("the document parses");
        assert!(
            triples
                .iter()
                .all(|triple| triple.predicate != discovered_operation),
            "no dp: document may declare a vg:discoveredOperation triple"
        );
    }

    // Every mapping's mapsContract names a MetadataContract node declared in
    // the same fixture — the contract is the only discovery entry point.
    for document in documents.iter().skip(2) {
        let triples = parse_turtle(document).expect("the fixture parses");
        let rdf_type = NamedNode::new(format!("{RDF}type")).unwrap();
        let metadata_contract = eg_rdf::oxrdf::Term::NamedNode(
            NamedNode::new(format!("{VG}MetadataContract")).unwrap(),
        );
        let contract_iris: Vec<String> = triples
            .iter()
            .filter(|triple| triple.predicate == rdf_type && triple.object == metadata_contract)
            .map(|triple| triple.subject.to_string())
            .collect();
        let maps_contract = NamedNode::new(format!("{VG}mapsContract")).unwrap();
        for triple in triples.iter().filter(|t| t.predicate == maps_contract) {
            assert!(
                contract_iris.contains(&triple.object.to_string()),
                "every vg:mapsContract must name a vg:MetadataContract node in the same fixture"
            );
        }
    }
}

/// Asserts every alignment row resolves: the package term lives in the
/// shared kg# namespace and the dp: object is declared in the
/// data-platform vocabulary (a class for rdfs:subClassOf rows, a property
/// for owl:equivalentProperty rows). Returns the row count.
fn assert_alignment_resolves(document: &str) -> usize {
    let triples = parse_turtle(document).expect("the alignment table parses");
    let vocabulary = load_data_platform();

    let sub_class_of = NamedNode::new(format!("{RDFS}subClassOf")).unwrap();
    let equivalent_property = NamedNode::new(format!("{OWL}equivalentProperty")).unwrap();
    let rdf_type = NamedNode::new(format!("{RDF}type")).unwrap();
    let kg_namespace = "http://knuckles.team/kg#";

    let mut rows = 0;
    for triple in triples.iter().filter(|t| {
        (t.predicate == sub_class_of || t.predicate == equivalent_property)
            && iri_of(&t.object).is_some_and(|iri| iri.starts_with(DP))
    }) {
        rows += 1;
        assert!(
            subject_iri_of(&triple.subject).starts_with(kg_namespace),
            "an alignment row names a package term in the shared kg# namespace: {}",
            triple.subject
        );
        let object_iri = iri_of(&triple.object).expect("the filter kept a named dp: object");
        let dp_local = object_iri[DP.len()..].to_string();
        let expected_types: Vec<String> = {
            let kinds: &[&str] = if triple.predicate == sub_class_of {
                &["Class"]
            } else {
                &["ObjectProperty", "DatatypeProperty"]
            };
            kinds.iter().map(|kind| format!("{OWL}{kind}")).collect()
        };
        assert!(
            vocabulary.iter().any(|t| {
                subject_iri_of(&t.subject) == format!("{DP}{dp_local}")
                    && t.predicate == rdf_type
                    && expected_types
                        .iter()
                        .any(|iri| iri_of(&t.object).as_deref() == Some(iri.as_str()))
            }),
            "{dp_local} must be declared in the data-platform vocabulary"
        );
    }
    rows
}

// spec: EG-DATAPLAT-VOCAB-R007.4
#[test]
fn virtual_default_for_all_eight() {
    // `all_virtual` marks the fixtures whose mappings are every virtual
    // (decision 4): sql, documentdb, objectstore, vector, openbao.
    let fixtures: &[(&str, bool)] = &[
        (
            include_str!("../ontology/fixtures/data_platform/sql.ttl"),
            true,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/documentdb.ttl"),
            true,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/objectstore.ttl"),
            true,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/vector.ttl"),
            true,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/kafka.ttl"),
            false,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/openbao.ttl"),
            true,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/clarity.ttl"),
            false,
        ),
        (
            include_str!("../ontology/fixtures/data_platform/data-science.ttl"),
            false,
        ),
    ];

    let store_mapping = NamedNode::new(format!("{DP}StoreMapping")).unwrap();
    let acceleration_mode = NamedNode::new(format!("{DP}accelerationMode")).unwrap();
    let materialisation_reason = NamedNode::new(format!("{DP}materialisationReason")).unwrap();

    for (document, all_virtual) in fixtures {
        let triples = parse_turtle(document).expect("the fixture parses");
        let rdf_type = NamedNode::new(format!("{RDF}type")).unwrap();
        let mapping_iris: Vec<String> = triples
            .iter()
            .filter(|triple| {
                triple.predicate == rdf_type
                    && triple.object == eg_rdf::oxrdf::Term::NamedNode(store_mapping.clone())
            })
            .map(|triple| triple.subject.to_string())
            .collect();
        assert!(
            !mapping_iris.is_empty(),
            "every fixture carries at least one StoreMapping"
        );

        for mapping_iri in &mapping_iris {
            let mode = triples
                .iter()
                .find(|triple| {
                    triple.subject.to_string() == *mapping_iri
                        && triple.predicate == acceleration_mode
                })
                .and_then(|triple| literal_value(&triple.object).map(|value| value.to_string()));
            assert!(
                matches!(mode.as_deref(), Some("virtual") | Some("materialised")),
                "every mapping names an accelerationMode: {mapping_iri}"
            );
            let has_reason = triples.iter().any(|triple| {
                triple.subject.to_string() == *mapping_iri
                    && triple.predicate == materialisation_reason
            });
            let has_acceleration_policy = triples.iter().any(|triple| {
                triple.subject.to_string() == *mapping_iri
                    && triple.predicate.as_str().contains("ccelerationPolicy")
            });

            match mode.as_deref() {
                Some("virtual") => assert!(
                    !has_reason && !has_acceleration_policy,
                    "a virtual mapping carries no materialisationReason and no AccelerationPolicy: {mapping_iri}"
                ),
                Some("materialised") => {
                    let reason = triples
                        .iter()
                        .find(|triple| {
                            triple.subject.to_string() == *mapping_iri
                                && triple.predicate == materialisation_reason
                        })
                        .and_then(|triple| {
                            literal_value(&triple.object).map(|value| value.to_string())
                        })
                        .unwrap_or_default();
                    assert!(
                        reason.contains("R038"),
                        "a materialised mapping cites R038 in its reason: {mapping_iri} ({reason})"
                    );
                }
                _ => panic!("mapping {mapping_iri} has no accelerationMode"),
            }
        }

        if *all_virtual {
            let any_materialised = mapping_iris.iter().any(|iri| {
                triples.iter().any(|triple| {
                    triple.subject.to_string() == *iri
                        && triple.predicate == acceleration_mode
                        && literal_value(&triple.object) == Some("materialised")
                })
            });
            assert!(
                !any_materialised,
                "an all-virtual fixture must carry no materialised mapping"
            );
        }
    }
}

// spec: EG-DATAPLAT-VOCAB-R008.1
#[test]
fn alignment_relational_resolves() {
    let document = include_str!("../ontology/fixtures/data_platform/alignment_relational.ttl");
    assert!(
        assert_alignment_resolves(document) >= 4,
        "the relational alignment carries at least 4 rows"
    );

    let triples = parse_turtle(document).expect("the alignment table parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#DatabaseTable",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}Table")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#isPrimaryKey",
        &format!("{OWL}equivalentProperty"),
        &format!("{DP}isPrimaryKey")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#Collection",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}Collection")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#documentCount",
        &format!("{OWL}equivalentProperty"),
        &format!("{DP}documentCount")
    ));
}

// spec: EG-DATAPLAT-VOCAB-R008.2
#[test]
fn alignment_object_vector_resolves() {
    let document = include_str!("../ontology/fixtures/data_platform/alignment_object_vector.ttl");
    assert!(
        assert_alignment_resolves(document) >= 4,
        "the object/vector alignment carries at least 4 rows"
    );

    let triples = parse_turtle(document).expect("the alignment table parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#Object",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}StoredObject")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#byteSize",
        &format!("{OWL}equivalentProperty"),
        &format!("{DP}byteSize")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#VectorCollection",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}VectorCollection")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#dimensions",
        &format!("{OWL}equivalentProperty"),
        &format!("{DP}dimensions")
    ));
}

// spec: EG-DATAPLAT-VOCAB-R008.3
#[test]
fn alignment_ops_ml_resolves() {
    let document = include_str!("../ontology/fixtures/data_platform/alignment_ops_ml.ttl");
    assert!(
        assert_alignment_resolves(document) >= 4,
        "the ops/ML alignment carries at least 4 rows"
    );

    let triples = parse_turtle(document).expect("the alignment table parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#ConsumerGroup",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}ConsumerGroup")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#BehaviorInsight",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}BehaviorMetric")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#TrainingRun",
        &format!("{RDFS}subClassOf"),
        &format!("{DP}TrainingRun")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg#trainedOn",
        &format!("{OWL}equivalentProperty"),
        &format!("{DP}trainedOn")
    ));
}
