//! Native parser contract for the shared data-platform vocabulary.

use eg_core::graph::{current_core_set_digest, GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};
use eg_shacl::validate_turtle;

const DP: &str = "http://knuckles.team/kg/data-platform#";
const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

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
    let range_iri = if range.starts_with("xsd:") {
        format!("{XSD}{}", &range[4..])
    } else {
        format!("{DP}{range}")
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

const DP_SHAPES: &str = include_str!("../ontology/data_platform-v1.shapes.ttl");
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

/// Validates a mapping fixture against the data-platform and virtual-graph
/// shapes and returns its triples.
fn validate_fixture(document: &str) -> Vec<Triple> {
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), document)
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
    let via_operation = NamedNode::new(&format!("{DP}viaOperation")).unwrap();
    for operation in operations {
        assert!(
            triples.iter().any(|triple| {
                triple.predicate == via_operation
                    && triple.object == eg_rdf::oxrdf::Term::Literal(
                        eg_rdf::oxrdf::Literal::new_simple_literal(*operation)
                    )
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

// spec: EG-DATAPLAT-VOCAB-R005.1
#[test]
fn secret_metadata_terms_present() {
    let triples = load_data_platform();
    assert!(has_iri(
        &triples,
        &format!("{DP}SecretEngine"),
        &format!("{RDFS}subClassOf"),
        &format!("{DP}DataStore")
    ));
    for class in ["SecretPath", "AccessPolicy", "RotationState", "PolicyPathGrant"] {
        assert_typed(&triples, class, "Class");
    }
    assert_typed(&triples, "hasRotationState", "ObjectProperty");
    for property in [
        "lastRotatedAt",
        "currentVersion",
        "leaseExpiresAt",
        "grantsCapability",
    ] {
        assert_typed(&triples, property, "DatatypeProperty");
    }
    assert_domain_range(&triples, "pathName", "SecretPath", "xsd:string");
    assert_domain_range(&triples, "hasRotationState", "SecretPath", "RotationState");
    assert_domain_range(&triples, "lastRotatedAt", "RotationState", "xsd:dateTime");
    assert_domain_range(&triples, "currentVersion", "RotationState", "xsd:integer");
    assert_domain_range(&triples, "leaseExpiresAt", "RotationState", "xsd:dateTime");
    assert_domain_range(&triples, "grantPolicy", "PolicyPathGrant", "AccessPolicy");
    assert_domain_range(&triples, "grantPath", "PolicyPathGrant", "SecretPath");
    assert_domain_range(&triples, "grantsCapability", "PolicyPathGrant", "xsd:string");
}

// spec: EG-DATAPLAT-VOCAB-R005.2
#[test]
fn secret_path_with_extra_property_rejected() {
    // A clean secret path — its name plus the store link, nothing else — conforms.
    let clean = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:engine a dp:SecretEngine .

ex:path a dp:SecretPath ;
    dp:pathName "secret/data/production/db" ;
    dp:inStore ex:engine .
"#;
    let report =
        validate_turtle(DP_SHAPES, clean).expect("the clean secret path parses before validation");
    assert!(
        report.conforms,
        "a secret path carrying only its name and store link must conform: {:?}",
        report.results
    );

    // The same path with one extra property — a pasted value — is a violation:
    // the closed shape names exactly the properties a secret path may carry.
    let extra = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:engine a dp:SecretEngine .

ex:path a dp:SecretPath ;
    dp:pathName "secret/data/production/db" ;
    dp:inStore ex:engine ;
    ex:secret "c2VjcmV0IHZhbHVl" .
"#;
    let report =
        validate_turtle(DP_SHAPES, extra).expect("the extra-property document parses before validation");
    assert!(
        !report.conforms,
        "a secret path with any extra property must be rejected: {:?}",
        report.results
    );

    // Closed holds for the other secret classes too: an access policy is the
    // name and the store link — never a rule body.
    let policy_with_rules = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix ex: <http://example.org/data-platform/> .

ex:engine a dp:SecretEngine .

ex:policy a dp:AccessPolicy ;
    dp:inStore ex:engine ;
    ex:rules "path \"secret/production/*\" { capability \"read\" }" .
"#;
    let report = validate_turtle(DP_SHAPES, policy_with_rules)
        .expect("the rule-body document parses before validation");
    assert!(
        !report.conforms,
        "an access policy carrying a rule body must be rejected: {:?}",
        report.results
    );
}

// spec: EG-DATAPLAT-VOCAB-R005.3
#[test]
fn secret_value_refused_and_no_value_property() {
    // A SecretPath carrying a value triple fails validation: the closed shape
    // names the metadata a secret path may carry, and a value is not among it.
    let with_value = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix kg: <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/data-platform/> .

ex:path a dp:SecretPath ;
    dp:pathName "secret/data/production/db" ;
    kg:value "s3cr3t" .
"#;
    let report =
        validate_turtle(DP_SHAPES, with_value).expect("the value document parses before validation");
    assert!(
        !report.conforms,
        "a secret path with a value triple must be rejected: {:?}",
        report.results
    );

    // A token triple is refused the same way.
    let with_token = r#"
@prefix dp: <http://knuckles.team/kg/data-platform#> .
@prefix kg: <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/data-platform/> .

ex:path a dp:SecretPath ;
    dp:pathName "secret/data/production/db" ;
    kg:token "tok-abc" .
"#;
    let report =
        validate_turtle(DP_SHAPES, with_token).expect("the token document parses before validation");
    assert!(
        !report.conforms,
        "a secret path with a token triple must be rejected: {:?}",
        report.results
    );

    // And the refusal cannot be undone by adding a property: neither the
    // vocabulary nor its shapes declare anything named value, secret, token,
    // password, ciphertext or rules.
    let vocabulary = load_data_platform();
    let shapes = parse_turtle(DP_SHAPES).expect("the shapes document parses");
    for document in [&vocabulary, &shapes] {
        for name in ["value", "secret", "token", "password", "ciphertext", "rules"] {
            let iri = format!("{DP}{name}");
            assert!(
                !has_iri(
                    document,
                    &iri,
                    &format!("{RDF}type"),
                    &format!("{OWL}DatatypeProperty")
                )
                && !has_iri(
                    document,
                    &iri,
                    &format!("{RDF}type"),
                    &format!("{OWL}ObjectProperty")
                ),
                "{name} must not be a declared data-platform property"
            );
        }
    }
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
        let report =
            validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &document)
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
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &good)
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

// spec: EG-DATAPLAT-VOCAB-R006.1
#[test]
fn mapping_fixture_sql_validates() {
    let triples =
        validate_fixture(include_str!("../ontology/fixtures/data_platform/sql.ttl"));
    assert_via_operations(
        &triples,
        &[
            "sql_schema:tables",
            "sql_schema:columns",
            "sql_schema:foreign_keys",
            "sql_schema:constraints",
        ],
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
    let triples =
        validate_fixture(include_str!("../ontology/fixtures/data_platform/vector.ttl"));
    assert_via_operations(
        &triples,
        &["vector_collection_management:list_collections"],
    );
}

// spec: EG-DATAPLAT-VOCAB-R006.5
#[test]
fn mapping_fixture_kafka_validates() {
    let triples =
        validate_fixture(include_str!("../ontology/fixtures/data_platform/kafka.ttl"));
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
    let triples =
        validate_fixture(include_str!("../ontology/fixtures/data_platform/openbao.ttl"));
    assert_via_operations(
        &triples,
        &["openbao_mcp_sys:get_mounts", "openbao_mcp_logical:list"],
    );
}

// spec: EG-DATAPLAT-VOCAB-R006.7
#[test]
fn mapping_fixture_clarity_validates() {
    let triples =
        validate_fixture(include_str!("../ontology/fixtures/data_platform/clarity.ttl"));
    assert_via_operations(&triples, &["clarity_insights:get_data_export"]);
}

// spec: EG-DATAPLAT-VOCAB-R006.8
#[test]
fn mapping_fixture_data_science_validates() {
    let triples = validate_fixture(include_str!(
        "../ontology/fixtures/data_platform/data-science.ttl"
    ));
    assert_via_operations(
        &triples,
        &["load_dataset", "dataset_lineage", "rank_models", "evaluate_model", "fit_model:fit"],
    );
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
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &no_reason)
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
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &with_reason)
            .expect("the with-reason mapping parses before validation");
    assert!(
        report.conforms,
        "a materialised mapping with a reason must conform: {:?}",
        report.results
    );

    // A virtual mapping needs no reason.
    let virtual_mode = mapping_with_mode("virtual", None);
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &virtual_mode)
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
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &missing_operation)
            .expect("the missing-operation mapping parses before validation");
    assert!(
        !report.conforms,
        "an mcp-operation mapping without a viaOperation must be rejected: {:?}",
        report.results
    );

    // The same mapping with its named operation conforms.
    let named_operation = mapping_with_kind("mcp-operation", Some("openbao_mcp_logical:list"));
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &named_operation)
            .expect("the named-operation mapping parses before validation");
    assert!(
        report.conforms,
        "an mcp-operation mapping with a viaOperation must conform: {:?}",
        report.results
    );

    // Other contract kinds do not require a viaOperation.
    let r2rml = mapping_with_kind("r2rml-sql", None);
    let report =
        validate_turtle(&format!("{DP_SHAPES}{VG_SHAPES}"), &r2rml)
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
    let discovered_operation =
        NamedNode::new(&format!("{VG}discoveredOperation")).unwrap();
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
        let rdf_type = NamedNode::new(&format!("{RDF}type")).unwrap();
        let metadata_contract =
            eg_rdf::oxrdf::Term::NamedNode(NamedNode::new(&format!("{VG}MetadataContract")).unwrap());
        let contract_iris: Vec<String> = triples
            .iter()
            .filter(|triple| {
                triple.predicate == rdf_type && triple.object == metadata_contract
            })
            .map(|triple| triple.subject.to_string())
            .collect();
        let maps_contract = NamedNode::new(&format!("{VG}mapsContract")).unwrap();
        for triple in triples.iter().filter(|t| t.predicate == maps_contract) {
            assert!(
                contract_iris.contains(&triple.object.to_string()),
                "every vg:mapsContract must name a vg:MetadataContract node in the same fixture"
            );
        }
    }
}

// spec: EG-DATAPLAT-VOCAB-R007.4
#[test]
fn virtual_default_for_all_eight() {
    // `all_virtual` marks the fixtures whose mappings are every virtual
    // (decision 4): sql, documentdb, objectstore, vector, openbao.
    let fixtures: &[(&str, bool)] = &[
        (include_str!("../ontology/fixtures/data_platform/sql.ttl"), true),
        (
            include_str!("../ontology/fixtures/data_platform/documentdb.ttl"),
            true,
        ),
        (include_str!("../ontology/fixtures/data_platform/objectstore.ttl"), true),
        (include_str!("../ontology/fixtures/data_platform/vector.ttl"), true),
        (include_str!("../ontology/fixtures/data_platform/kafka.ttl"), false),
        (include_str!("../ontology/fixtures/data_platform/openbao.ttl"), true),
        (include_str!("../ontology/fixtures/data_platform/clarity.ttl"), false),
        (
            include_str!("../ontology/fixtures/data_platform/data-science.ttl"),
            false,
        ),
    ];

    let store_mapping = NamedNode::new(&format!("{DP}StoreMapping")).unwrap();
    let acceleration_mode = NamedNode::new(&format!("{DP}accelerationMode")).unwrap();
    let materialisation_reason =
        NamedNode::new(&format!("{DP}materialisationReason")).unwrap();

    for (document, all_virtual) in fixtures {
        let triples = parse_turtle(document).expect("the fixture parses");
        let rdf_type = NamedNode::new(&format!("{RDF}type")).unwrap();
        let mapping_iris: Vec<String> = triples
            .iter()
            .filter(|triple| {
                triple.predicate == rdf_type
                    && triple
                        .object
                        == eg_rdf::oxrdf::Term::NamedNode(store_mapping.clone())
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
                .and_then(|triple| {
                    literal_value(&triple.object).map(|value| value.to_string())
                });
            assert!(
                matches!(mode.as_deref(), Some("virtual") | Some("materialised")),
                "every mapping names an accelerationMode: {mapping_iri}"
            );
            let has_reason = triples.iter().any(|triple| {
                triple.subject.to_string() == *mapping_iri
                    && triple.predicate == materialisation_reason
            });
            let has_acceleration_policy = triples
                .iter()
                .any(|triple| {
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
