//! Native ontology coverage for the Q7 agent-graph slot edge.
#![cfg(feature = "owl")]

mod common;

use common::{has_iri, parse_all};
use medulla_rdf::owl::Reasoner;
use medulla_rdf::oxrdf::Triple;

const SWARM: &str = "http://knuckles.team/kg/swarm#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";

fn documents() -> Vec<Triple> {
    parse_all(&[include_str!("../../medulla-core/ontology/swarm_topology-v1.ttl")])
}

// spec: MEDULLA-CQ-ORCH-R007.1
#[test]
fn slot_feeds_edge_resolves_and_reasoner_stays_consistent() {
    let mut triples = documents();
    let feeds = format!("{SWARM}feeds");
    let slot = format!("{SWARM}Slot");
    assert!(has_iri(&triples, &feeds, RDF_TYPE, OWL_OBJECT_PROPERTY));
    assert!(has_iri(&triples, &feeds, &format!("{RDFS}domain"), &slot));
    assert!(has_iri(&triples, &feeds, &format!("{RDFS}range"), &slot));

    triples.extend(parse_all(&[
        "@prefix swarm: <http://knuckles.team/kg/swarm#> .\n\
             swarm:source a swarm:Slot ; swarm:feeds swarm:target .\n\
             swarm:target a swarm:Slot .",
    ]));
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
}
