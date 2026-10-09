//! `core:swarm-topology@1` + `core:swarm-topology-shapes@1` (SWARM-TOPOLOGY-DECIDE-DESIGN
//! §4, ST-1): the swarm
//! vocabulary and its template-projection shapes are an EG core source, so every
//! graph's composed schema carries them. The SHACL fixtures: a well-formed template
//! projection conforms under the COMPOSED shapes (the shapes-omitted
//! `ShaclValidate` path) and each planted defect is flagged.

use super::test_support::conforms;
use crate::graph::GraphSchemaSources;

const SWARM_SOURCE: &str = "core:swarm-topology@1";
const SWARM_SHAPES: &str = "core:swarm-topology-shapes@1";

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

#[test]
fn the_swarm_vocabulary_and_shapes_are_core_sources() {
    let sources = GraphSchemaSources::default();
    let vocabulary = sources
        .core
        .get(SWARM_SOURCE)
        .expect("the swarm vocabulary");
    assert!(vocabulary
        .ontology_ttl
        .as_deref()
        .is_some_and(|d| d.contains("egowl:admitsTopology")));
    let shapes = sources.core.get(SWARM_SHAPES).expect("the swarm shapes");
    assert!(shapes
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

/// A template for a non-fan-out, non-peer-team shape: one or more slots
/// whose roles are never `Child` (that role's SPARQL check is scoped to any
/// slot with that role, not only `FanOutJoin`) and never trigger the
/// peer-team round check (`PeerTeam`, `Debate`, `Council`, `CritiqueLoop`).
fn plain_projection(class: &str, slots: &[(&str, &str)], stop: &str) -> String {
    let slot_iris: Vec<String> = (0..slots.len()).map(|i| format!("<urn:s{i}>")).collect();
    let mut data = format!(
        "@prefix swarm: <http://knuckles.team/kg/swarm#> .\n\
         <urn:t> a swarm:TopologyTemplate ; swarm:class swarm:{class} ;\n\
           swarm:hasSlot {} ; swarm:hasStopRule <urn:stop> .\n",
        slot_iris.join(", ")
    );
    for (i, (role, node_id)) in slots.iter().enumerate() {
        data += &format!(
            "<urn:s{i}> a swarm:Slot ; swarm:nodeId \"{node_id}\" ; swarm:role swarm:{role} ;\n\
               swarm:minWidth 1 ; swarm:maxWidth 1 ; swarm:maxRounds 1 .\n"
        );
    }
    data += &format!("<urn:stop> a swarm:StopRule, {stop} .\n");
    data
}

/// A peer-team template (`Council`, `CritiqueLoop`): `Peer` slots, plus a
/// `Verifier` slot when the stop rule is `VerifierPassStop`.
fn peer_projection(class: &str, peers: u8, with_verifier: bool, stop: &str) -> String {
    let mut slots: Vec<(&str, &str)> = (0..peers).map(|_| ("Peer", "peer")).collect();
    if with_verifier {
        slots.push(("Verifier", "verifier"));
    }
    plain_projection(class, &slots, stop)
}

#[test]
fn every_standard_shape_has_a_conforming_template() {
    // EG-DECISION-ENGINE-R106: the standard agent arrangement shapes --
    // single-agent, pipeline, fan-out/join, supervisor-to-workers,
    // critique-loop and council -- each validate against the swarm-topology
    // SHACL shapes with a minimal, otherwise-unremarkable template.
    let single = plain_projection("Single", &[("Parent", "solo")], MAX_ROUNDS);
    assert!(conforms(&single), "Single must conform");

    let pipeline = plain_projection(
        "Pipeline",
        &[("Parent", "stage1"), ("Aggregator", "stage2")],
        MAX_ROUNDS,
    );
    assert!(conforms(&pipeline), "Pipeline must conform");

    assert!(
        conforms(&projection(FAN, (1, 4), MAX_ROUNDS, "gpu")),
        "FanOutJoin must conform"
    );

    let supervisor_workers = plain_projection(
        "SupervisorWorkers",
        &[("Parent", "supervisor"), ("Peer", "worker")],
        MAX_ROUNDS,
    );
    assert!(
        conforms(&supervisor_workers),
        "SupervisorWorkers must conform"
    );

    let critique_loop = peer_projection("CritiqueLoop", 1, true, "swarm:VerifierPassStop");
    assert!(conforms(&critique_loop), "CritiqueLoop must conform");

    let council = peer_projection(
        "Council",
        3,
        false,
        "swarm:QuorumStop ; swarm:k 2 ; swarm:n 3",
    );
    assert!(conforms(&council), "Council must conform");
}
