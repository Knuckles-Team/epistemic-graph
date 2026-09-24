//! `core:swarm-topology@1` (SWARM-TOPOLOGY-DECIDE-DESIGN §4, ST-1): the swarm
//! vocabulary and its template-projection shapes are an EG core source, so every
//! graph's composed schema carries them. The SHACL fixtures: a well-formed template
//! projection conforms under the COMPOSED shapes (the shapes-omitted
//! `ShaclValidate` path) and each planted defect is flagged.

use super::compose::validate_and_compose;
use crate::graph::GraphSchemaSources;

const SWARM_SOURCE: &str = "core:swarm-topology@1";

/// A fan-out/join template: lead (parent) → fanout → worker (child, 1..4) → join.
fn projection(fan_nodes: &str, worker_widths: (u8, u8), stop: &str, resource: &str) -> String {
    format!(
        "@prefix swarm: <http://knuckles.team/kg/swarm#> .\n\
         <urn:t> a swarm:TopologyTemplate ; swarm:class swarm:FanOutJoin{fan_nodes} ;\n\
           swarm:hasSlot <urn:lead>, <urn:worker> ; swarm:hasStopRule <urn:stop> .\n\
         <urn:lead> a swarm:Slot ; swarm:nodeId \"lead\" ; swarm:role swarm:Parent ;\n\
           swarm:minWidth 1 ; swarm:maxWidth 1 ; swarm:maxRounds 1 ; swarm:lease <urn:l0> .\n\
         <urn:l0> a swarm:LeaseDemand ; swarm:resourceClass \"{resource}\" ; swarm:amount 1 .\n\
         <urn:worker> a swarm:Slot ; swarm:nodeId \"worker\" ; swarm:role swarm:Child ;\n\
           swarm:minWidth {} ; swarm:maxWidth {} ; swarm:maxRounds 1 .\n\
         <urn:stop> a swarm:StopRule, {stop} .\n",
        worker_widths.0, worker_widths.1
    )
}

const FAN: &str = " ; swarm:nodeKind \"fanout\", \"join\"";
const MAX_ROUNDS: &str = "swarm:MaxRoundsStop ; swarm:n 1";

fn conforms(data: &str) -> bool {
    let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
    let data = eg_shacl::graph_from_turtle(data).unwrap();
    eg_shacl::validate(&composed.shapes, &data)
        .unwrap()
        .conforms
}

#[test]
fn the_swarm_vocabulary_and_shapes_are_one_core_source() {
    let sources = GraphSchemaSources::default();
    let source = sources
        .core
        .get(SWARM_SOURCE)
        .expect("the swarm core source");
    assert!(source
        .ontology_ttl
        .as_deref()
        .is_some_and(|d| d.contains("eg:admitsTopology")));
    assert!(source
        .shapes_ttl
        .as_deref()
        .is_some_and(|d| d.contains("TemplateTopologyShape")));
}

#[test]
fn a_well_formed_template_projection_conforms() {
    assert!(conforms(&projection(
        FAN,
        (1, 4),
        MAX_ROUNDS,
        "llm_generator"
    )));
}

#[test]
fn each_planted_template_defect_is_flagged() {
    let plants = [
        (
            "no join",
            projection(" ; swarm:nodeKind \"fanout\"", (1, 4), MAX_ROUNDS, "gpu"),
        ),
        ("inverted width", projection(FAN, (3, 2), MAX_ROUNDS, "gpu")),
        (
            "unknown resource",
            projection(FAN, (1, 4), MAX_ROUNDS, "quantum"),
        ),
        (
            "half quorum",
            projection(FAN, (1, 4), "swarm:QuorumStop ; swarm:k 2", "gpu"),
        ),
        (
            "k above n",
            projection(
                FAN,
                (1, 4),
                "swarm:QuorumStop ; swarm:k 3 ; swarm:n 2",
                "gpu",
            ),
        ),
        (
            "verifier pass without verifier",
            projection(FAN, (1, 4), "swarm:VerifierPassStop", "gpu"),
        ),
    ];
    for (name, data) in plants {
        assert!(!conforms(&data), "{name} must not conform");
    }
}
