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
    dp:viaOperation "{via_operation}" .
"#
    )
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
