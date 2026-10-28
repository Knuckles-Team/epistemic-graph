//! Native parser contract for the shared data-platform vocabulary.

use eg_core::graph::{current_core_set_digest, GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

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
            Term::BlankNode(node) => Some(format!("_:{}", node.as_str())),
            // RDF-star quoted triples are not object values for these assertions.
            Term::Triple(_) => None,
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
    store_mapping_fixture(
        include_str!("../ontology/fixtures/data_platform/data-science.ttl"),
        &[
            "load_dataset",
            "dataset_lineage",
            "rank_models",
            "evaluate_model",
        ],
    );
}
