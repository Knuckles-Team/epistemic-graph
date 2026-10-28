//! Native parser contract for the shared data-platform vocabulary.

use eg_core::graph::{current_core_set_digest, GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Triple};

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
