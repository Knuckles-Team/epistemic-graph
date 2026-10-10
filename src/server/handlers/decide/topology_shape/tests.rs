use eg_types::agent_graph::{AgentGraphEdge, AgentGraphNode, AgentGraphShape};
use eg_types::contract::BoundedVec;
use eg_types::test_support::topology::{facts, slot};

use super::*;

fn node(node_id: &str, kind: AgentGraphNodeKind) -> AgentGraphNode {
    AgentGraphNode {
        node_id: node_id.to_string(),
        kind,
        deps_contract: None,
        output_contract: None,
    }
}

fn agent(node_id: &str) -> AgentGraphNode {
    node(
        node_id,
        AgentGraphNodeKind::Agent {
            agent_id: format!("agent-{node_id}"),
            definition_digest: format!("sha256:{}", "1".repeat(64)),
        },
    )
}

fn edge(from: &str, to: &str) -> AgentGraphEdge {
    AgentGraphEdge {
        from: from.to_string(),
        to: to.to_string(),
        condition: None,
    }
}

/// lead → fanout → worker → join → end, carrying the fixture's fan-out facts.
fn fan_out_draft() -> AgentGraphDraft {
    AgentGraphDraft {
        graph_id: "graph:fan".to_string(),
        version: "1".to_string(),
        shape: AgentGraphShape {
            entry_node: "lead".to_string(),
            nodes: vec![
                agent("lead"),
                node("fan", AgentGraphNodeKind::Fanout),
                agent("worker"),
                node("join", AgentGraphNodeKind::Join),
                node("end", AgentGraphNodeKind::End),
            ],
            edges: vec![
                edge("lead", "fan"),
                edge("fan", "worker"),
                edge("worker", "join"),
                edge("join", "end"),
            ],
            max_iterations: 8,
        },
        tenant_id: "tenant-a".to_string(),
        actor_scope: "operator".to_string(),
        purpose_id: "agent-graph:publish".to_string(),
        policy_digest: format!("sha256:{}", "2".repeat(64)),
        synthesis_evidence: None,
        topology: Some(facts()),
    }
}

#[test]
fn a_well_formed_fan_out_template_passes() {
    assert_eq!(refusal(&fan_out_draft()), None);
    let mut plain = fan_out_draft();
    plain.topology = None;
    assert_eq!(
        refusal(&plain),
        None,
        "a graph without topology facts is not checked"
    );
}

// spec: EG-DECISION-ENGINE-R108
#[test]
fn every_planted_shape_defect_is_refused_by_name() {
    type Plant = (&'static str, fn(&mut AgentGraphDraft));
    let plants: &[Plant] = &[
        ("agent nodes", |d| d.shape.nodes.push(agent("stray"))),
        ("Fanout node", |d| {
            d.shape.nodes.retain(|n| n.node_id != "join")
        }),
        ("more rounds", |d| {
            let facts = d.topology.as_mut().unwrap();
            let mut slots = facts.slots.as_slice().to_vec();
            slots[0].max_rounds = 2;
            facts.slots = BoundedVec::new(slots).unwrap();
        }),
        ("verifier slot", |d| {
            d.topology.as_mut().unwrap().stop = StopRule::VerifierPass { max_rounds: 1 };
        }),
        ("peer voters", |d| {
            d.topology.as_mut().unwrap().stop = StopRule::Quorum { k: 1, n: 2 };
        }),
        ("depth", |d| {
            d.shape.nodes.push(node(
                "child",
                AgentGraphNodeKind::Graph {
                    graph_id: "graph:child".to_string(),
                    shape_digest: format!("sha256:{}", "3".repeat(64)),
                },
            ));
        }),
        ("choices", |d| {
            let facts = d.topology.as_mut().unwrap();
            facts.slots = BoundedVec::new(vec![
                slot("lead", SlotRole::Parent, (1, 1), None),
                slot("worker", SlotRole::Child, (1, 12), None),
            ])
            .unwrap();
        }),
    ];
    for (needle, plant) in plants {
        let mut draft = fan_out_draft();
        plant(&mut draft);
        let refused = refusal(&draft).unwrap_or_else(|| panic!("{needle}: not refused"));
        assert!(refused.starts_with(TOPOLOGY_SHAPE_INVALID), "{refused}");
        assert!(refused.contains(needle), "{needle}: {refused}");
    }
}
