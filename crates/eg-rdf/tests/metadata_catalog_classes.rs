//! The metadata-catalog core vocabulary: catalogued assets, owners and stewards,
//! lineage, the glossary and system-prompt candidates, aligned to W3C DCAT,
//! PROV-O, ORG and SKOS (MEDULLA-TYPED-PACKS-R106.1, MEDULLA-DECISION-ENGINE-R814.1.1).
#![cfg(feature = "owl")]

mod common;

use common::{has_iri, parse_all, BFO, KG, RDFS};
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::Triple;

const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const CAT: &str = "http://knuckles.team/kg/catalog#";
const CP: &str = "http://knuckles.team/kg/connector-pack#";
const METADATA_CATALOG: &str = "http://knuckles.team/kg/metadata_catalog";
const DCAT: &str = "http://www.w3.org/ns/dcat#";
const PROV: &str = "http://www.w3.org/ns/prov#";
const ORG: &str = "http://www.w3.org/ns/org#";
const SKOS: &str = "http://www.w3.org/2004/02/skos/core#";
const SHAPES: &str = include_str!("../../eg-core/ontology/metadata_catalog-v1.shapes.ttl");

fn documents() -> Vec<Triple> {
    parse_all(&[
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/capability-v1.ttl"),
        include_str!("../../eg-core/ontology/company-v1.ttl"),
        include_str!("../../eg-core/ontology/software-v1.ttl"),
        include_str!("../../eg-core/ontology/sdlc_lifecycle-v1.ttl"),
        include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../eg-core/ontology/trm-v1.ttl"),
        include_str!("../../eg-core/ontology/orchestration-v1.ttl"),
        include_str!("../../eg-core/ontology/archimate-v1.ttl"),
        include_str!("../../eg-core/ontology/sdd-v1.ttl"),
        include_str!("../../eg-core/ontology/service_operations-v1.ttl"),
        include_str!("../../eg-core/ontology/metadata_catalog-v1.ttl"),
    ])
}

// spec: MEDULLA-TYPED-PACKS-R106.1
// spec: MEDULLA-DECISION-ENGINE-R814.1
// spec: MEDULLA-DECISION-ENGINE-R814.1.1
#[test]
fn catalog_ontology_declares_root_and_cross_domain_imports() {
    let triples = documents();
    assert!(has_iri(
        &triples,
        METADATA_CATALOG,
        &format!("{RDF}type"),
        &format!("{OWL}Ontology")
    ));
    for imported in [
        "http://knuckles.team/kg/core",
        "http://knuckles.team/kg/capability",
        "http://knuckles.team/kg/orchestration",
        "http://knuckles.team/kg/service_operations",
    ] {
        assert!(has_iri(
            &triples,
            METADATA_CATALOG,
            &format!("{OWL}imports"),
            imported
        ));
    }
}

// spec: MEDULLA-TYPED-PACKS-R106.1
// spec: MEDULLA-DECISION-ENGINE-R814.1.1
#[test]
fn catalog_classes_roll_up_to_eg_core_and_align_to_w3c() {
    let triples = documents();
    let owl_class = format!("{OWL}Class");
    let sub = format!("{RDFS}subClassOf");
    let expected: [(String, &[String]); 16] = [
        (
            format!("{CAT}Catalog"),
            &[format!("{CAT}Asset"), format!("{DCAT}Catalog")],
        ),
        (
            format!("{CAT}Asset"),
            &[format!("{BFO}0000031"), format!("{DCAT}Resource")],
        ),
        (
            format!("{CAT}DataResource"),
            &[
                format!("{CAT}Asset"),
                format!("{KG}Dataset"),
                format!("{DCAT}Dataset"),
            ],
        ),
        (format!("{CAT}Field"), &[format!("{CAT}Asset")]),
        (format!("{CAT}ToolAsset"), &[format!("{CAT}Asset")]),
        (
            format!("{CAT}PromptAsset"),
            &[format!("{CAT}Asset"), format!("{KG}Prompt")],
        ),
        (
            format!("{KG}PromptCandidate"),
            &[format!("{CAT}PromptAsset")],
        ),
        (
            format!("{CAT}SkillAsset"),
            &[format!("{CAT}Asset"), format!("{KG}Document")],
        ),
        (
            format!("{CAT}AgentAsset"),
            &[format!("{CAT}Asset"), format!("{KG}Document")],
        ),
        (
            format!("{CP}Connector"),
            &[format!("{KG}Tool"), format!("{DCAT}DataService")],
        ),
        (
            format!("{CAT}StewardshipAssignment"),
            &[format!("{BFO}0000031"), format!("{PROV}Attribution")],
        ),
        (
            format!("{CAT}StewardshipRole"),
            &[format!("{KG}Role"), format!("{ORG}Role")],
        ),
        (
            format!("{CAT}LineageEdge"),
            &[format!("{BFO}0000031"), format!("{PROV}Derivation")],
        ),
        (
            format!("{CAT}IngestRun"),
            &[format!("{BFO}0000015"), format!("{PROV}Activity")],
        ),
        (
            format!("{CAT}Glossary"),
            &[format!("{BFO}0000031"), format!("{SKOS}ConceptScheme")],
        ),
        (
            format!("{CAT}GlossaryTerm"),
            &[format!("{KG}Concept"), format!("{SKOS}Concept")],
        ),
    ];
    for (class, parents) in &expected {
        assert!(
            has_iri(&triples, class, &format!("{RDF}type"), &owl_class),
            "{class}"
        );
        for parent in *parents {
            assert!(
                has_iri(&triples, class, &sub, parent),
                "{class} under {parent}"
            );
        }
    }
    assert!(has_iri(
        &triples,
        &format!("{KG}AssignmentGroup"),
        &sub,
        &format!("{ORG}OrganizationalUnit")
    ));

    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(
        classification.unsatisfiable.is_empty(),
        "unsatisfiable: {:?}",
        classification.unsatisfiable
    );
    // Every catalog class reaches the BFO root through EG core, never through a
    // W3C parent alone (packs and core modules roll up to EG core).
    let entity = format!("<{BFO}0000001>");
    for (class, _) in &expected {
        let subsumers = &classification.subsumers[&format!("<{class}>")];
        assert!(
            subsumers.contains(&entity),
            "{class} does not reach BFO: {subsumers:?}"
        );
    }
}

// spec: MEDULLA-TYPED-PACKS-R106.1
#[test]
fn owner_steward_and_lineage_properties_specialise_prov() {
    let triples = documents();
    let sub = format!("{RDFS}subPropertyOf");
    let expected = [
        (format!("{CAT}hasOwner"), format!("{KG}ownedBy")),
        (format!("{CAT}hasOwner"), format!("{PROV}wasAttributedTo")),
        (format!("{CAT}hasSteward"), format!("{PROV}wasAttributedTo")),
        (format!("{CP}declaredOwner"), format!("{KG}ownedBy")),
        (format!("{CAT}principal"), format!("{PROV}agent")),
        (format!("{CAT}role"), format!("{PROV}hadRole")),
        (format!("{KG}derivedFrom"), format!("{PROV}wasDerivedFrom")),
        (format!("{CAT}fromSource"), format!("{KG}derivedFrom")),
        (format!("{KG}memberOfGroup"), format!("{ORG}memberOf")),
    ];
    for (property, parent) in expected {
        assert!(
            has_iri(&triples, &property, &sub, &parent),
            "{property} under {parent}"
        );
    }
    assert!(has_iri(
        &triples,
        &format!("{CAT}lineageTo"),
        &format!("{OWL}inverseOf"),
        &format!("{KG}derivedFrom")
    ));
    for role in ["owner", "steward", "custodian"] {
        assert!(has_iri(
            &triples,
            &format!("{CAT}{role}"),
            &format!("{RDF}type"),
            &format!("{CAT}StewardshipRole")
        ));
    }
}

const PREFIXES: &str = "@prefix : <http://knuckles.team/kg#> . \
     @prefix cat: <http://knuckles.team/kg/catalog#> . \
     @prefix cp: <http://knuckles.team/kg/connector-pack#> . \
     @prefix skos: <http://www.w3.org/2004/02/skos/core#> . ";

fn conforms(data: &str) -> bool {
    eg_shacl::validate_turtle(SHAPES, &format!("{PREFIXES}{data}"))
        .unwrap()
        .conforms
}

const DIGEST: &str = "0000000000000000000000000000000000000000000000000000000000000000";

// spec: MEDULLA-TYPED-PACKS-R106.1
// spec: MEDULLA-DECISION-ENGINE-R814.1.1
#[test]
fn catalog_shapes_accept_a_complete_catalog() {
    let data = format!(
        "<urn:c:sn> a cp:Connector . \
         <urn:a:incidents> a cat:DataResource ; cp:ofConnector <urn:c:sn> ; cp:servesClass :Incident ; \
             cat:hasField <urn:a:incidents#group> . \
         <urn:a:incidents#group> a cat:Field ; cp:ofConnector <urn:c:sn> ; cp:servesProperty cat:hasSteward . \
         <urn:a:get_incidents> a cat:ToolAsset ; cp:ofConnector <urn:c:sn> ; cp:digest \"{DIGEST}\" . \
         <urn:a:responder> a :PromptCandidate, cat:PromptAsset ; cp:ofConnector <urn:c:sn> ; \
             :systemPromptFor <urn:task:incident-triage> . \
         <urn:s:1> a cat:StewardshipAssignment ; cat:assignedTo <urn:a:incidents> ; \
             cat:principal <urn:g:service-desk> ; cat:role cat:owner . \
         <urn:l:1> a cat:LineageEdge ; cat:from <urn:a:get_incidents> ; cat:to <urn:a:incidents> ; \
             cat:hop \"resource\" . \
         <urn:t:incident> a cat:GlossaryTerm ; skos:prefLabel \"Incident\" ; cat:definesClass :Incident ."
    );
    assert!(conforms(&data));
}

// spec: MEDULLA-TYPED-PACKS-R106.1
// spec: MEDULLA-DECISION-ENGINE-R814.1.1
#[test]
fn catalog_shapes_refuse_incomplete_or_open_vocabulary_records() {
    // An asset with no connector, or two.
    assert!(!conforms("<urn:a:t> a cat:ToolAsset ."));
    assert!(!conforms(
        "<urn:a:t> a cat:ToolAsset ; cp:ofConnector <urn:c:a>, <urn:c:b> ."
    ));
    // A field also belongs to exactly one connector-backed catalog.
    assert!(!conforms(
        "<urn:a:f> a cat:Field ; cp:servesProperty cat:hasSteward ."
    ));
    assert!(!conforms(
        "<urn:a:f> a cat:Field ; cp:ofConnector <urn:c:a>, <urn:c:b> ; \
         cp:servesProperty cat:hasSteward ."
    ));
    // A data resource that serves no class.
    assert!(!conforms(
        "<urn:a:r> a cat:DataResource ; cp:ofConnector <urn:c:a> ."
    ));
    // A digest that is not sha256 hex.
    assert!(!conforms(
        "<urn:a:t> a cat:ToolAsset ; cp:ofConnector <urn:c:a> ; cp:digest \"abc\" ."
    ));
    // A system-prompt candidate that frames no task.
    assert!(!conforms(
        "<urn:a:p> a :PromptCandidate, cat:PromptAsset ; cp:ofConnector <urn:c:a> ."
    ));
    // A stewardship role outside the closed set.
    assert!(!conforms(
        "<urn:s:1> a cat:StewardshipAssignment ; cat:assignedTo <urn:a:r> ; \
         cat:principal <urn:g:x> ; cat:role cat:approver ."
    ));
    // A lineage hop outside the closed set.
    assert!(!conforms(
        "<urn:l:1> a cat:LineageEdge ; cat:from <urn:a:x> ; cat:to <urn:a:y> ; cat:hop \"copy\" ."
    ));
    // A glossary term that names no class.
    assert!(!conforms(
        "<urn:t:x> a cat:GlossaryTerm ; skos:prefLabel \"X\" ."
    ));
    // An unknown asset flag.
    assert!(!conforms(
        "<urn:a:t> a cat:ToolAsset ; cp:ofConnector <urn:c:a> ; cat:flag \"stale\" ."
    ));
    // The owner_missing flag is part of the closed set.
    assert!(conforms(
        "<urn:a:t> a cat:ToolAsset ; cp:ofConnector <urn:c:a> ; cat:flag \"owner_missing\" ."
    ));
}
