//! Contract tests for the registered commercial core module and its shapes
//! companion (MEDULLA-SUPPLY-VOCAB-R003.13 to R003.16).

use std::sync::Arc;

use medulla_core::graph::{
    current_core_set_digest, GraphSchemaSource, GraphSchemaSources, SchemaSourceOrigin,
};
use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::oxrdf::NamedNode;

const KG: &str = "http://knuckles.team/kg#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
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

fn commercial_source() -> (GraphSchemaSources, Arc<str>, Arc<str>) {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    let source = &sources.core["core:commercial@1"];
    let ontology = source
        .ontology_ttl
        .clone()
        .expect("the commercial module carries an ontology document");
    let shapes = source
        .shapes_ttl
        .clone()
        .expect("the commercial module carries its shapes companion");
    (sources, ontology, shapes)
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.13
#[test]
fn commercial_module_loads() {
    let (sources, ontology, _shapes) = commercial_source();
    // The module loads through the core catalog under its one canonical id.
    assert_eq!(
        sources
            .core
            .keys()
            .filter(|id| id.starts_with("core:commercial@"))
            .count(),
        1,
        "commercial is registered exactly once"
    );
    let triples = parse_turtle(&ontology).expect("the commercial ontology parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/commercial",
        RDF_TYPE,
        &format!("{OWL}Ontology")
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
        catalog_document.contains("<http://knuckles.team/kg/commercial>"),
        "the catalog lists the commercial import"
    );
    // A missing registration is refused.
    let mut missing = sources.clone();
    missing.core.remove("core:commercial@1");
    assert!(missing
        .validate()
        .unwrap_err()
        .contains("immutable catalog"));
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.14
#[test]
fn commercial_shapes_load() {
    let (sources, _ontology, shapes) = commercial_source();
    // The registered companion parses and carries the contract and capability shapes.
    let triples = parse_turtle(&shapes).expect("the commercial shapes parse");
    for shape in ["CustomerContractShape", "SupportsCapabilityShape"] {
        assert!(
            has_iri(
                &triples,
                &format!("{KG}{shape}"),
                RDF_TYPE,
                &format!("{SHACL}NodeShape")
            ),
            "{shape} is a registered node shape"
        );
    }
    // Overwriting the registered shapes document is refused by the core catalog.
    let mut overwritten = sources.clone();
    let replaced = GraphSchemaSource::new(
        SchemaSourceOrigin::Core {
            module: "commercial".to_string(),
            version: 1,
            set_digest: current_core_set_digest(),
        },
        Some(Arc::from("# overwritten shapes\n")),
        Some(
            overwritten.core["core:commercial@1"]
                .ontology_ttl
                .clone()
                .unwrap(),
        ),
        0,
    )
    .expect("a document pair always constructs");
    overwritten
        .core
        .insert("core:commercial@1".to_string(), replaced);
    assert!(overwritten
        .validate()
        .unwrap_err()
        .contains("immutable catalog"));
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.15
#[test]
fn commercial_shared_namespace() {
    let (_sources, ontology, _shapes) = commercial_source();
    let triples = parse_turtle(&ontology).expect("the commercial ontology parses");
    // Exact canonical IRIs in the shared kg namespace.
    for class in ["Customer", "CustomerContract", "Opportunity"] {
        assert!(
            has_iri(
                &triples,
                &format!("{KG}{class}"),
                RDF_TYPE,
                &format!("{OWL}Class")
            ),
            "{class}"
        );
    }
    for property in ["opportunityCustomer", "coversService"] {
        assert!(has_iri(
            &triples,
            &format!("{KG}{property}"),
            RDF_TYPE,
            &format!("{OWL}ObjectProperty")
        ));
    }
    // No duplicate incompatible declarations: the shared Supplier class is
    // reused from its own module, never redeclared or narrowed here.
    for forbidden in ["Supplier", "CommerceFile"] {
        let subject = NamedNode::new(format!("{KG}{forbidden}")).unwrap();
        assert!(
            !triples
                .iter()
                .any(|triple| triple.subject == subject.clone().into()),
            "no {forbidden} declaration in the commercial module"
        );
    }
    // No accidental domain narrowing of a shared property: the commercial
    // module never constrains the shared suppliedBy/ownedBy domains.
    for shared in ["suppliedBy", "ownedBy"] {
        let subject = NamedNode::new(format!("{KG}{shared}")).unwrap();
        assert!(
            !triples
                .iter()
                .any(|triple| triple.subject == subject.clone().into()),
            "no {shared} redeclaration in the commercial module"
        );
    }
    // The existing supplier and customer fixtures remain valid in the shared namespace.
    let fixture = parse_turtle(include_str!("fixtures/commercial_shared.ttl"))
        .expect("the shared fixture parses");
    assert!(has_iri(
        &fixture,
        "http://knuckles.team/test/commercial#acmeSupplier",
        RDF_TYPE,
        &format!("{KG}Supplier")
    ));
    assert!(has_iri(
        &fixture,
        "http://knuckles.team/test/commercial#acmeCustomer",
        RDF_TYPE,
        &format!("{KG}Customer")
    ));
}

// spec: MEDULLA-SUPPLY-VOCAB-R003.16
#[test]
fn commercial_close_date_declared() {
    let (_sources, ontology, _shapes) = commercial_source();
    let triples = parse_turtle(&ontology).expect("the commercial ontology parses");
    let close_date = format!("{KG}closeDate");
    assert!(has_iri(
        &triples,
        &close_date,
        RDF_TYPE,
        &format!("{OWL}DatatypeProperty")
    ));
    assert!(has_iri(
        &triples,
        &close_date,
        &format!("{RDFS}range"),
        &format!("{XSD}dateTime")
    ));
    assert!(has_iri(
        &triples,
        &close_date,
        &format!("{RDFS}domain"),
        &format!("{KG}Opportunity")
    ));
}

const EX: &str = "http://knuckles.team/test/commercial#";

// spec: MEDULLA-SUPPLY-VOCAB-R003.17
#[test]
fn commercial_opportunity_date_shape() {
    let (_sources, _ontology, shapes) = commercial_source();
    // The registered shape accepts a valid dated opportunity.
    let valid = medulla_shacl::validate_turtle(
        &shapes,
        &format!(
            "@prefix kg: <{KG}> .\n\
             @prefix ex: <{EX}> .\n\
             @prefix xsd: <{XSD}> .\n\
             ex:openDeal a kg:Opportunity ;\n\
                 kg:closeDate \"2026-11-30T00:00:00Z\"^^xsd:dateTime .\n"
        ),
    )
    .expect("the registered shapes and a dated opportunity parse");
    assert!(
        valid.conforms,
        "a dated opportunity conforms: {:?}",
        valid.results
    );
    // A missing closeDate is refused.
    let missing = medulla_shacl::validate_turtle(
        &shapes,
        &format!(
            "@prefix kg: <{KG}> .\n\
             @prefix ex: <{EX}> .\n\
             ex:openDeal a kg:Opportunity .\n"
        ),
    )
    .expect("the registered shapes and the undated opportunity parse");
    assert!(!missing.conforms, "an undated opportunity must be refused");
    assert!(
        missing.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .map(|path| path.trim_matches(['<', '>']))
                == Some(&format!("{KG}closeDate"))
        }),
        "the refusal names the closeDate path: {:?}",
        missing.results
    );
    // A close date with a non-dateTime datatype is refused.
    let bad_type = medulla_shacl::validate_turtle(
        &shapes,
        &format!(
            "@prefix kg: <{KG}> .\n\
             @prefix ex: <{EX}> .\n\
             ex:openDeal a kg:Opportunity ;\n\
                 kg:closeDate \"30/11/2026\" .\n"
        ),
    )
    .expect("the registered shapes and the bad-typed opportunity parse");
    assert!(
        !bad_type.conforms,
        "a non-dateTime close date must be refused"
    );
    assert!(
        bad_type.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .map(|path| path.trim_matches(['<', '>']))
                == Some(&format!("{KG}closeDate"))
        }),
        "the refusal names the closeDate path: {:?}",
        bad_type.results
    );
}

// spec: MEDULLA-SUPPLY-VOCAB-R001.1.2
#[test]
fn supply_chain_module_loads_through_default_catalog() {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    let source = sources
        .core
        .get("core:supply_chain@1")
        .expect("the supply-chain vocabulary is registered exactly once");
    let ontology = source
        .ontology_ttl
        .as_deref()
        .expect("the supply-chain module carries an ontology document");
    let triples = parse_turtle(ontology).expect("the supply-chain ontology parses");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/supply_chain",
        RDF_TYPE,
        &format!("{OWL}Ontology")
    ));
    for class in [
        "Supplier",
        "SoftwareComponent",
        "ComponentVersion",
        "Vulnerability",
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{KG}{class}"),
                RDF_TYPE,
                &format!("{OWL}Class")
            ),
            "{class} is exposed by the registered default catalog"
        );
    }
    for property in ["usesComponent", "publishedBy", "containsComponent"] {
        assert!(
            has_iri(
                &triples,
                &format!("{KG}{property}"),
                RDF_TYPE,
                &format!("{OWL}ObjectProperty")
            ),
            "{property} is exposed by the registered default catalog"
        );
    }
    let catalog = sources
        .core
        .get("core:catalog@1")
        .and_then(|source| source.ontology_ttl.as_deref())
        .expect("the aggregate catalog carries an ontology document");
    assert!(catalog.contains("<http://knuckles.team/kg/supply_chain>"));
}
