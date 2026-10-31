//! Contract tests for the registered connector-pack core module: the
//! ingest-contract vocabulary (MEDULLA-TYPED-PACKS-R101.1) and the connector
//! surface vocabulary (MEDULLA-TYPED-PACKS-R102.1).

use std::sync::Arc;

use medulla_core::graph::GraphSchemaSources;
use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::oxrdf::NamedNode;

const CP: &str = "http://knuckles.team/kg/connector-pack#";
const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const KG: &str = "http://knuckles.team/kg#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const SHACL: &str = "http://www.w3.org/ns/shacl#";

fn has_iri(
    triples: &[medulla_rdf::oxrdf::Triple],
    subject: &str,
    predicate: &str,
    object: &str,
) -> bool {
    triples.contains(&medulla_rdf::oxrdf::Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn connector_pack_source() -> (GraphSchemaSources, Arc<str>, Arc<str>) {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    let source = &sources.core["core:connector-pack@1"];
    let ontology = source
        .ontology_ttl
        .clone()
        .expect("the connector-pack module carries an ontology document");
    let shapes = source
        .shapes_ttl
        .clone()
        .expect("the connector-pack module carries its shapes companion");
    (sources, ontology, shapes)
}

// spec: MEDULLA-TYPED-PACKS-R101.1
#[test]
fn contract_terms_resolve_with_domain_and_range() {
    let (sources, ontology, _shapes) = connector_pack_source();
    // The module loads through the core catalog under its one canonical id.
    assert_eq!(
        sources
            .core
            .keys()
            .filter(|id| id.starts_with("core:connector-pack@"))
            .count(),
        1,
        "connector-pack is registered exactly once"
    );
    let triples = parse_turtle(&ontology).expect("the connector-pack ontology parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/connector-pack",
        RDF_TYPE,
        &format!("{OWL}Ontology")
    ));
    // It imports the virtual-graph vocabulary the contract terms specialise.
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/connector-pack",
        OWL_IMPORTS,
        "http://knuckles.team/kg/virtual-graph"
    ));
    // The regenerated aggregate catalog lists the module.
    let catalog = sources
        .core
        .get("core:catalog@1")
        .expect("the catalog core source is present");
    let catalog_document = catalog
        .ontology_ttl
        .as_deref()
        .expect("the catalog carries its aggregate document");
    assert!(
        catalog_document.contains("<http://knuckles.team/kg/connector-pack>"),
        "the catalog lists the connector-pack import"
    );
    // The contract classes are declared, and the three that specialise
    // virtual-graph classes do so by the exact IRI.
    for class in [
        "IngestContract",
        "SourceObject",
        "FieldMapping",
        "KeyRule",
        "CursorPolicy",
        "AccessPolicy",
        "PrivacyClass",
        "ProvenanceRequirement",
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{class}"),
                RDF_TYPE,
                &format!("{OWL}Class")
            ),
            "{class}"
        );
    }
    assert!(has_iri(
        &triples,
        &format!("{CP}IngestContract"),
        &format!("{RDFS}subClassOf"),
        &format!("{VG}MetadataContract")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}SourceObject"),
        &format!("{RDFS}subClassOf"),
        &format!("{VG}VirtualMapping")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}FieldMapping"),
        &format!("{RDFS}subClassOf"),
        &format!("{VG}PredicateFieldPair")
    ));
    // Every contract property resolves with its declared domain and range.
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    let object_properties = [
        ("contractOf", "IngestContract", "Connector"),
        ("hasSourceObject", "IngestContract", "SourceObject"),
        ("hasKeyRule", "SourceObject", "KeyRule"),
        ("hasCursorPolicy", "IngestContract", "CursorPolicy"),
        ("hasAccessPolicy", "IngestContract", "AccessPolicy"),
        (
            "requiresProvenance",
            "IngestContract",
            "ProvenanceRequirement",
        ),
    ];
    for (property, property_domain, property_range) in object_properties {
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                RDF_TYPE,
                &format!("{OWL}ObjectProperty")
            ),
            "{property}"
        );
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                &domain,
                &format!("{CP}{property_domain}")
            ),
            "{property} domain"
        );
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                &range,
                &format!("{CP}{property_range}")
            ),
            "{property} range"
        );
    }
    let datatype_properties = [
        "contractVersion",
        "contractDigest",
        "cursorKind",
        "defaultAcl",
        "aclField",
        "tenantField",
        "readRole",
        "privacyClass",
    ];
    for property in datatype_properties {
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                RDF_TYPE,
                &format!("{OWL}DatatypeProperty")
            ),
            "{property}"
        );
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                &range,
                &format!("{XSD}string")
            ),
            "{property} range"
        );
    }
    // cp:governedBy links an ingested node (any mapped class) to its contract.
    assert!(has_iri(
        &triples,
        &format!("{CP}governedBy"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}governedBy"),
        &range,
        &format!("{CP}IngestContract")
    ));
}

// spec: MEDULLA-TYPED-PACKS-R102.1
#[test]
fn surface_terms_resolve_with_domain_and_range() {
    let (_sources, ontology, shapes) = connector_pack_source();
    let triples = parse_turtle(&ontology).expect("the connector-pack ontology parses");
    // The surface classes are declared; the tool, prompt and skill
    // specialise the core classes by IRI.
    for class in ["Connector", "Feature", "Tool", "Prompt", "Skill"] {
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{class}"),
                RDF_TYPE,
                &format!("{OWL}Class")
            ),
            "{class}"
        );
    }
    assert!(has_iri(
        &triples,
        &format!("{CP}Tool"),
        &format!("{RDFS}subClassOf"),
        &format!("{KG}Tool")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}Prompt"),
        &format!("{RDFS}subClassOf"),
        &format!("{KG}Prompt")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}Skill"),
        &format!("{RDFS}subClassOf"),
        &format!("{KG}Skill")
    ));
    // The surface properties resolve with their declared domain and range.
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    let object_properties = [
        ("providesFeature", "Connector"),
        ("hasContract", "Connector"),
        ("realizedBy", "Feature"),
        ("operatesOn", "Tool"),
        ("produces", "Tool"),
        ("executes", "Tool"),
        ("providesCapability", "Tool"),
    ];
    for (property, property_domain) in object_properties {
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                RDF_TYPE,
                &format!("{OWL}ObjectProperty")
            ),
            "{property}"
        );
        assert!(
            has_iri(
                &triples,
                &format!("{CP}{property}"),
                &domain,
                &format!("{CP}{property_domain}")
            ),
            "{property} domain"
        );
    }
    // The concrete declared ranges (realizedBy and appliesTo carry union
    // ranges; cp:componentId carries a union domain).
    assert!(has_iri(
        &triples,
        &format!("{CP}providesFeature"),
        &range,
        &format!("{CP}Feature")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}hasContract"),
        &range,
        &format!("{CP}IngestContract")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}operatesOn"),
        &range,
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}executes"),
        &range,
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}appliesTo"),
        &range,
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}providesCapability"),
        &range,
        &format!("{KG}ServiceCapability")
    ));
    // cp:componentId stamps one stable id on every surface node.
    assert!(has_iri(
        &triples,
        &format!("{CP}componentId"),
        RDF_TYPE,
        &format!("{OWL}DatatypeProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{CP}componentId"),
        &range,
        &format!("{XSD}string")
    ));
    // The registered shapes companion carries the surface node shapes.
    let shape_triples = parse_turtle(&shapes).expect("the connector-pack shapes parse");
    for shape in ["ToolShape", "FeatureShape"] {
        assert!(
            has_iri(
                &shape_triples,
                &format!("{CP}{shape}"),
                RDF_TYPE,
                &format!("{SHACL}NodeShape")
            ),
            "{shape} is a registered node shape"
        );
    }
}
