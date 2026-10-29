//! Native-parser contract for the shared supply-chain vocabulary declarations.
#![cfg(feature = "rdf")]

mod common;

use common::{has_iri, KG, RDFS};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term};

const DOCUMENT: &str = include_str!("../../eg-core/ontology/supply_chain-v1.ttl");
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// The named classes a `subject predicate` value stands for: the named objects
/// themselves, plus every member of an `owl:unionOf` list.
fn union_members(triples: &[eg_rdf::oxrdf::Triple], subject: &str, predicate: &str) -> Vec<String> {
    let mut found = Vec::new();
    for triple in triples.iter().filter(|t| {
        t.subject == NamedNode::new(subject).unwrap().into() && t.predicate.as_str() == predicate
    }) {
        match &triple.object {
            Term::NamedNode(node) => found.push(node.as_str().to_string()),
            Term::BlankNode(node) => {
                // The blank node is either a union filler (its `unionOf`
                // value is the list head) or, for `propertyChainAxiom`,
                // the list head itself.
                let node_term = NamedOrBlankNode::BlankNode(node.clone());
                let list_head = triples
                    .iter()
                    .find(|t| {
                        t.subject == node_term.clone()
                            && t.predicate.as_str() == format!("{OWL}unionOf")
                    })
                    .map(|t| t.object.clone())
                    .unwrap_or(Term::BlankNode(node.clone()));
                let mut list = Some(list_head);
                while let Some(Term::BlankNode(cell)) = list {
                    let cell = NamedOrBlankNode::BlankNode(cell);
                    for t in triples.iter().filter(|t| t.subject == cell) {
                        if t.predicate.as_str() == format!("{RDF}first") {
                            if let Term::NamedNode(n) = &t.object {
                                found.push(n.as_str().to_string());
                            }
                        }
                    }
                    list = triples
                        .iter()
                        .find(|t| t.subject == cell && t.predicate.as_str() == format!("{RDF}rest"))
                        .map(|t| t.object.clone());
                }
            }
            _ => {}
        }
    }
    found.sort();
    found
}

// spec: EG-SUPPLY-VOCAB-R001.1.1
#[test]
fn shared_classes_use_existing_iris_and_declared_superclasses() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    for (class, parent) in [
        ("Supplier", format!("{KG}Vendor")),
        (
            "SupplyChainItem",
            "http://purl.obolibrary.org/obo/BFO_0000031".into(),
        ),
    ] {
        let subject = format!("{KG}{class}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}Class")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &format!("{RDFS}subClassOf"),
            &parent
        ));
        assert!(triples.iter().any(|triple| {
            triple.subject == NamedNode::new(&subject).unwrap().into()
                && triple.predicate.as_str() == format!("{RDFS}label")
                && triple.object.is_literal()
        }));
    }
}

// spec: EG-SUPPLY-VOCAB-R001.1.1
#[test]
fn supplies_item_has_supplier_domain_and_item_range() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    for (predicate, object) in [
        (RDF_TYPE.to_string(), format!("{OWL}ObjectProperty")),
        (format!("{RDFS}domain"), format!("{KG}Supplier")),
        (format!("{RDFS}range"), format!("{KG}SupplyChainItem")),
    ] {
        assert!(has_iri(
            &triples,
            &format!("{KG}suppliesItem"),
            &predicate,
            &object
        ));
    }
}

// spec: EG-SUPPLY-VOCAB-R001.1.1
#[test]
fn shared_file_does_not_redeclare_existing_supplied_by_property() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let existing = NamedNode::new(format!("{KG}suppliedBy")).unwrap();
    assert!(!triples
        .iter()
        .any(|triple| triple.subject == existing.clone().into()));
}

// spec: EG-CQ-XSRC-R002.1
#[test]
fn supplier_and_item_terms_are_listed() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    for (class, parent) in [
        ("Supplier", format!("{KG}Vendor")),
        (
            "SupplyChainItem",
            "http://purl.obolibrary.org/obo/BFO_0000031".into(),
        ),
    ] {
        let subject = format!("{KG}{class}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}Class")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &format!("{RDFS}subClassOf"),
            &parent
        ));
    }
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesItem"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesItem"),
        &format!("{RDFS}domain"),
        &format!("{KG}Supplier")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesItem"),
        &format!("{RDFS}range"),
        &format!("{KG}SupplyChainItem")
    ));
}

// spec: EG-SUPPLY-VOCAB-R001.2
#[test]
fn realized_by_asset_and_asset_host_declare_domain_and_range() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    for (property, domain_iri, range_iri) in [
        (
            "realizedByAsset",
            format!("{KG}SupplyChainItem"),
            format!("{KG}AssetInstance"),
        ),
        (
            "assetHost",
            format!("{KG}AssetInstance"),
            format!("{INFRA}Host"),
        ),
    ] {
        let subject = format!("{KG}{property}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}ObjectProperty")
        ));
        assert!(has_iri(&triples, &subject, &domain, &domain_iri));
        assert!(has_iri(&triples, &subject, &range, &range_iri));
    }
}

// spec: EG-SUPPLY-VOCAB-R002.1
#[test]
fn software_component_terms_declared() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    // SoftwareComponent reuses the IRI the SDD module already declares. Both
    // it and the new ComponentVersion are information-bearing continuants.
    for (class, parent) in [
        (
            "SoftwareComponent",
            "http://purl.obolibrary.org/obo/BFO_0000031",
        ),
        (
            "ComponentVersion",
            "http://purl.obolibrary.org/obo/BFO_0000031",
        ),
    ] {
        let subject = format!("{KG}{class}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}Class")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &format!("{RDFS}subClassOf"),
            parent
        ));
    }
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    assert!(has_iri(
        &triples,
        &format!("{KG}supplyChainVersionOf"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}supplyChainVersionOf"),
        &format!("{RDFS}subPropertyOf"),
        &format!("{KG}versionOf")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}supplyChainVersionOf"),
        &domain,
        &format!("{KG}ComponentVersion")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}supplyChainVersionOf"),
        &range,
        &format!("{KG}SoftwareComponent")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}versionString"),
        RDF_TYPE,
        &format!("{OWL}DatatypeProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}versionString"),
        &domain,
        &format!("{KG}ComponentVersion")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}versionString"),
        &range,
        &format!("{XSD}string")
    ));
}

// spec: EG-SUPPLY-VOCAB-R002.3
#[test]
fn contains_component_and_published_by_declared() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    for (property, domain_iri, range_iri) in [
        (
            "containsComponent",
            format!("{INFRA}ContainerImage"),
            format!("{KG}ComponentVersion"),
        ),
        (
            "publishedBy",
            format!("{KG}SoftwareComponent"),
            format!("{KG}Supplier"),
        ),
    ] {
        let subject = format!("{KG}{property}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}ObjectProperty")
        ));
        assert!(has_iri(&triples, &subject, &domain, &domain_iri));
        assert!(has_iri(&triples, &subject, &range, &range_iri));
    }
}

// spec: EG-SUPPLY-VOCAB-R002.4
#[test]
fn uses_image_shares_the_container_manager_iri_and_deployed_as_declared() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    // The IRI is the one the container-manager ontology declares (container.ttl),
    // in the shared kg namespace, not a per-module redefinition.
    assert_eq!(
        format!("{KG}usesImage"),
        format!("http://knuckles.team/kg#usesImage")
    );
    let subject = format!("{KG}usesImage");
    assert!(has_iri(
        &triples,
        &subject,
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &subject,
        &format!("{RDFS}range"),
        &format!("{INFRA}ContainerImage")
    ));
    assert_eq!(
        union_members(&triples, &subject, &format!("{RDFS}domain")),
        vec![format!("{INFRA}Container"), format!("{INFRA}Workload")]
    );
    let deployed_as = format!("{KG}deployedAs");
    assert!(has_iri(
        &triples,
        &deployed_as,
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &deployed_as,
        &format!("{RDFS}domain"),
        &format!("{INFRA}Service")
    ));
    assert!(has_iri(
        &triples,
        &deployed_as,
        &format!("{RDFS}range"),
        &format!("{INFRA}Workload")
    ));
}

// spec: EG-SUPPLY-VOCAB-R002.5
#[test]
fn uses_component_declares_the_runtime_chain_axiom() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let subject = format!("{KG}usesComponent");
    assert!(has_iri(
        &triples,
        &subject,
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &subject,
        &format!("{RDFS}domain"),
        &format!("{INFRA}Service")
    ));
    assert!(has_iri(
        &triples,
        &subject,
        &format!("{RDFS}range"),
        &format!("{KG}ComponentVersion")
    ));
    let chain = union_members(&triples, &subject, &format!("{OWL}propertyChainAxiom"));
    assert_eq!(
        chain,
        vec![
            format!("{KG}containsComponent"),
            format!("{KG}deployedAs"),
            format!("{KG}usesImage"),
        ]
    );
}

// spec: EG-CQ-XSRC-R002.3
#[test]
fn vulnerability_and_affects_component_terms_are_listed() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    assert!(has_iri(
        &triples,
        &format!("{KG}Vulnerability"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    assert!(has_iri(
        &triples,
        &format!("{KG}affectsComponent"),
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}affectsComponent"),
        &domain,
        &format!("{KG}Vulnerability")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}affectsComponent"),
        &range,
        &format!("{KG}ComponentVersion")
    ));
    // The trm count property is left unchanged: trm still declares only the
    // count, and does not gain the :Vulnerability class this file owns.
    let trm = parse_turtle(include_str!("../../eg-core/ontology/trm-v1.ttl")).unwrap();
    assert!(has_iri(
        &trm,
        &format!("{KG}vulnerabilityCount"),
        RDF_TYPE,
        &format!("{OWL}DatatypeProperty")
    ));
    assert!(!trm.iter().any(
        |triple| triple.subject == NamedNode::new(format!("{KG}Vulnerability")).unwrap().into()
    ));
}

/// The ordered steps of every `owl:propertyChainAxiom` list on `subject`;
/// an inverse step `[ owl:inverseOf p ]` appears as `inv:<p>`.
fn chain_steps(triples: &[eg_rdf::oxrdf::Triple], subject: &str) -> Vec<Vec<String>> {
    let mut chains = Vec::new();
    for head in triples.iter().filter(|t| {
        t.subject == NamedNode::new(subject).unwrap().into()
            && t.predicate.as_str() == format!("{OWL}propertyChainAxiom")
    }) {
        let mut steps = Vec::new();
        let mut list = Some(head.object.clone());
        while let Some(Term::BlankNode(cell)) = list {
            let cell = NamedOrBlankNode::BlankNode(cell);
            for t in triples.iter().filter(|t| t.subject == cell) {
                if t.predicate.as_str() != format!("{RDF}first") {
                    continue;
                }
                match &t.object {
                    Term::NamedNode(n) => steps.push(n.as_str().to_string()),
                    Term::BlankNode(b) => {
                        let inner = NamedOrBlankNode::BlankNode(b.clone());
                        for inv in triples.iter().filter(|x| {
                            x.subject == inner && x.predicate.as_str() == format!("{OWL}inverseOf")
                        }) {
                            if let Term::NamedNode(n) = &inv.object {
                                steps.push(format!("inv:{}", n.as_str()));
                            }
                        }
                    }
                    _ => {}
                }
            }
            list = triples
                .iter()
                .find(|t| t.subject == cell && t.predicate.as_str() == format!("{RDF}rest"))
                .map(|t| t.object.clone());
        }
        chains.push(steps);
    }
    chains
}

// spec: EG-CQ-XSRC-R004.1
#[test]
fn supplier_service_carries_the_software_chain_and_inverts_supplies_service() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let subject = format!("{KG}serviceSupplier");
    assert!(has_iri(
        &triples,
        &subject,
        RDF_TYPE,
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &subject,
        &format!("{OWL}inverseOf"),
        &format!("{KG}suppliesService")
    ));
    assert_eq!(
        chain_steps(&triples, &subject),
        vec![vec![
            format!("{KG}deployedAs"),
            format!("{KG}usesImage"),
            format!("{KG}containsComponent"),
            format!("{KG}supplyChainVersionOf"),
            format!("{KG}publishedBy"),
        ]]
    );
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn supplies_service_has_the_only_domain_and_range_and_no_chain_recurses() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    let supplies = format!("{KG}suppliesService");
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    assert!(has_iri(&triples, &supplies, &domain, &format!("{KG}Supplier")));
    assert!(has_iri(&triples, &supplies, &range, &format!("{INFRA}Service")));
    assert!(
        chain_steps(&triples, &supplies).is_empty(),
        "suppliesService carries no property chain"
    );
    let via_host = format!("{KG}suppliesServiceViaHost");
    assert!(has_iri(
        &triples,
        &via_host,
        &format!("{RDFS}subPropertyOf"),
        &supplies
    ));
    assert_eq!(
        chain_steps(&triples, &via_host),
        vec![vec![
            format!("{KG}suppliesItem"),
            format!("{KG}realizedByAsset"),
            format!("{KG}assetHost"),
            format!("inv:{INFRA}serviceRunsOn"),
        ]]
    );
    let family = [
        supplies.clone(),
        format!("{KG}serviceSupplier"),
        via_host.clone(),
    ];
    // Only suppliesService declares a domain or range.
    for member in &family[1..] {
        for predicate in [&domain, &range] {
            assert!(
                !triples.iter().any(|t| t.subject
                    == NamedNode::new(member.as_str()).unwrap().into()
                    && t.predicate.as_str() == predicate.as_str()),
                "{member} must not declare {predicate}"
            );
        }
    }
    // No chain on any property mentions a family member (no recursion).
    let owners: std::collections::BTreeSet<String> = triples
        .iter()
        .filter(|t| t.predicate.as_str() == format!("{OWL}propertyChainAxiom"))
        .map(|t| t.subject.to_string().trim_matches(|c| c == '<' || c == '>').to_string())
        .collect();
    for owner in owners {
        for steps in chain_steps(&triples, &owner) {
            for step in steps {
                let bare = step.trim_start_matches("inv:");
                assert!(
                    !family.iter().any(|m| m == bare),
                    "chain of {owner} mentions {bare}"
                );
            }
        }
    }
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn service_runs_on_is_a_service_scoped_runs_on() {
    let triples = parse_turtle(include_str!("../../eg-core/ontology/infrastructure-v1.ttl")).unwrap();
    let subject = format!("{INFRA}serviceRunsOn");
    assert!(has_iri(&triples, &subject, &format!("{RDFS}subPropertyOf"), &format!("{INFRA}runsOn")));
    assert!(has_iri(&triples, &subject, &format!("{RDFS}domain"), &format!("{INFRA}Service")));
    assert!(has_iri(&triples, &subject, &format!("{RDFS}range"), &format!("{INFRA}Host")));
}

// spec: EG-SUPPLY-VOCAB-R002.6
#[test]
fn vulnerability_terms_declared_and_trm_count_untouched() {
    let triples = parse_turtle(DOCUMENT).unwrap();
    // The class itself ships with the shared file; this row adds its
    // describing properties. trm does not declare :Vulnerability, so the
    // plan's conditional keeps the shipped BFO parent.
    assert!(has_iri(
        &triples,
        &format!("{KG}Vulnerability"),
        RDF_TYPE,
        &format!("{OWL}Class")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}Vulnerability"),
        &format!("{RDFS}subClassOf"),
        "http://purl.obolibrary.org/obo/BFO_0000031"
    ));
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    for name in ["cveId", "severity", "fixedInVersion"] {
        let subject = format!("{KG}{name}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}DatatypeProperty")
        ));
        assert!(has_iri(
            &triples,
            &subject,
            &domain,
            &format!("{KG}Vulnerability")
        ));
        assert!(has_iri(&triples, &subject, &range, &format!("{XSD}string")));
    }
    for (property, domain_iri, range_iri) in [
        (
            "affectsComponent",
            format!("{KG}Vulnerability"),
            format!("{KG}ComponentVersion"),
        ),
        (
            "hasVulnerability",
            format!("{INFRA}ContainerImage"),
            format!("{KG}Vulnerability"),
        ),
    ] {
        let subject = format!("{KG}{property}");
        assert!(has_iri(
            &triples,
            &subject,
            RDF_TYPE,
            &format!("{OWL}ObjectProperty")
        ));
        assert!(has_iri(&triples, &subject, &domain, &domain_iri));
        assert!(has_iri(&triples, &subject, &range, &range_iri));
    }
    // The trm count property is left unchanged.
    let trm = parse_turtle(include_str!("../../eg-core/ontology/trm-v1.ttl")).unwrap();
    assert!(has_iri(
        &trm,
        &format!("{KG}vulnerabilityCount"),
        RDF_TYPE,
        &format!("{OWL}DatatypeProperty")
    ));
}
