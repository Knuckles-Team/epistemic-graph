//! IT4IT-aligned service-operations core and the competency-question paths
//! (EG-TYPED-PACKS-R105.4, EG-TYPED-PACKS-R105.5).
#![cfg(feature = "owl")]

use eg_rdf::mapping::parse_turtle;
use eg_rdf::owl::Reasoner;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

const KG: &str = "http://knuckles.team/kg#";
const BFO: &str = "http://purl.obolibrary.org/obo/BFO_";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

fn documents() -> Vec<Triple> {
    [
        include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
        include_str!("../../eg-core/ontology/company-v1.ttl"),
        include_str!("../../eg-core/ontology/software-v1.ttl"),
        include_str!("../../eg-core/ontology/sdlc_lifecycle-v1.ttl"),
        include_str!("../../eg-core/ontology/infrastructure-v1.ttl"),
        include_str!("../../eg-core/ontology/trm-v1.ttl"),
        include_str!("../../eg-core/ontology/orchestration-v1.ttl"),
        include_str!("../../eg-core/ontology/archimate-v1.ttl"),
        include_str!("../../eg-core/ontology/sdd-v1.ttl"),
        include_str!("../../eg-core/ontology/service_operations-v1.ttl"),
    ]
    .into_iter()
    .flat_map(|document| parse_turtle(document).unwrap())
    .collect()
}

fn kg(name: &str) -> String {
    format!("{KG}{name}")
}

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

/// The named classes an `rdfs:domain` / `rdfs:range` value stands for: the class
/// itself, or every member of an `owl:unionOf` list.
fn members(triples: &[Triple], subject: &str, predicate: &str) -> Vec<String> {
    let mut found = Vec::new();
    for triple in triples.iter().filter(|t| {
        t.subject == NamedNode::new(subject).unwrap().into() && t.predicate.as_str() == predicate
    }) {
        match &triple.object {
            Term::NamedNode(node) => found.push(node.as_str().to_string()),
            Term::BlankNode(node) => {
                let mut list = triples
                    .iter()
                    .find(|t| {
                        t.subject == NamedOrBlankNode::BlankNode(node.clone())
                            && t.predicate.as_str() == format!("{OWL}unionOf")
                    })
                    .map(|t| t.object.clone());
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
    found
}

// spec: EG-TYPED-PACKS-R105.4
#[test]
fn service_operations_classes_exist_with_their_parents() {
    let triples = documents();
    let owl_class = format!("{OWL}Class");
    let sub = format!("{RDFS}subClassOf");
    let expected = [
        ("Issue", kg("WorkItem")),
        ("Ticket", kg("WorkItem")),
        ("Incident", kg("Ticket")),
        ("Problem", kg("Ticket")),
        ("Change", kg("Ticket")),
        ("ChangeRequest", kg("Change")),
        ("ConfigurationItem", kg("AssetInstance")),
        ("Application", kg("ConfigurationItem")),
        ("BusinessApplication", kg("Application")),
        ("BusinessService", kg("ConfigurationItem")),
        ("Server", kg("ConfigurationItem")),
        ("AssignmentGroup", kg("Organization")),
        ("TRMStandard", kg("TechnologyStandard")),
        ("TRMPhase", kg("Concept")),
        ("Task", format!("{BFO}0000015")),
        ("Prompt", format!("{BFO}0000031")),
        ("AssetInstance", format!("{BFO}0000004")),
    ];
    for (class, parent) in expected {
        let class = kg(class);
        assert!(has_iri(&triples, &class, &format!("{RDF}type"), &owl_class), "{class}");
        assert!(has_iri(&triples, &class, &sub, &parent), "{class} under {parent}");
    }
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
}

// spec: EG-TYPED-PACKS-R105.4
// spec: EG-TYPED-PACKS-R105.5
#[test]
fn competency_question_edges_declare_domain_and_range() {
    let triples = documents();
    let domain = format!("{RDFS}domain");
    let range = format!("{RDFS}range");
    let edges: [(&str, &str, &[&str], &[&str]); 8] = [
        ("affects", "ObjectProperty", &["Incident", "Problem", "Change"], &["ConfigurationItem"]),
        ("touches", "ObjectProperty", &["ChangeRequest"], &["ConfigurationItem"]),
        ("usesTechnology", "ObjectProperty", &["ConfigurationItem"], &["TechnologyStandard"]),
        ("hostedOn", "ObjectProperty", &["Application", "BusinessService"], &["Server"]),
        ("relatedTo", "ObjectProperty", &["Repository", "WorkItem", "Application"], &["Repository", "WorkItem", "Application"]),
        ("systemPromptFor", "ObjectProperty", &["Prompt"], &["Task"]),
        ("canHandle", "ObjectProperty", &["Skill", "Agent", "Prompt"], &["Task", "Incident"]),
        ("hasPhase", "ObjectProperty", &["TechnologyStandard"], &["TRMPhase"]),
    ];
    for (name, kind, domains, ranges) in edges {
        let property = kg(name);
        assert!(has_iri(&triples, &property, &format!("{RDF}type"), &format!("{OWL}{kind}")), "{name}");
        let mut got = members(&triples, &property, &domain);
        got.sort();
        let mut want: Vec<String> = domains.iter().map(|c| kg(c)).collect();
        want.sort();
        assert_eq!(got, want, "{name} domain");
        let mut got = members(&triples, &property, &range);
        got.sort();
        let mut want: Vec<String> = ranges.iter().map(|c| kg(c)).collect();
        want.sort();
        assert_eq!(got, want, "{name} range");
    }
    // Datatype properties: state and priority apply to work items.
    let datatype = format!("{OWL}DatatypeProperty");
    for name in ["state", "priority"] {
        let property = kg(name);
        assert!(has_iri(&triples, &property, &format!("{RDF}type"), &datatype), "{name}");
        assert!(members(&triples, &property, &domain).contains(&kg("WorkItem")), "{name} domain");
    }
    assert!(has_iri(&triples, &kg("relatedTo"), &format!("{RDF}type"), &format!("{OWL}SymmetricProperty")));
}

// spec: EG-TYPED-PACKS-R105.5
#[test]
fn self_contained_modules_align_to_the_core() {
    let triples = documents();
    let sub = format!("{RDFS}subClassOf");
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/swarm#TopologyTemplate",
        &sub,
        &kg("Template")
    ));
    assert!(has_iri(
        &triples,
        "http://knuckles.team/kg/virtual-graph#MetadataContract",
        &sub,
        &kg("Document")
    ));
}
