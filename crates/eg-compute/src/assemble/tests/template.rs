//! Template enumeration (DECIDE-LAYER-DESIGN §7.3): operator-published graph
//! templates are filled slot by slot, the best topology wins, and each losing
//! template is explained with its own optimum.

use eg_types::agent_graph::{AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind, AgentGraphShape};
use eg_types::decision::{DecisionOutcome, TemplateFacts};
use eg_types::delegation::AgentGraphEntryRef;

use super::super::{assemble, inputs as build_inputs, replay_check};
use super::fixture::*;

fn agent_node(node_id: &str) -> AgentGraphNode {
    AgentGraphNode {
        node_id: node_id.to_string(),
        kind: AgentGraphNodeKind::Agent {
            agent_id: format!("placeholder-{node_id}"),
            definition_digest: digest_of(node_id),
        },
        deps_contract: None,
        output_contract: None,
    }
}

fn edge(from: &str, to: &str) -> AgentGraphEdge {
    AgentGraphEdge {
        from: from.to_string(),
        to: to.to_string(),
        condition: None,
    }
}

/// A published chain of `agents` agent nodes ending in `End`.
fn template(graph_id: &str, agents: &[&str]) -> TemplateFacts {
    let mut nodes: Vec<AgentGraphNode> = agents.iter().map(|id| agent_node(id)).collect();
    nodes.push(AgentGraphNode {
        node_id: "end".to_string(),
        kind: AgentGraphNodeKind::End,
        deps_contract: None,
        output_contract: None,
    });
    let mut edges: Vec<AgentGraphEdge> = agents
        .windows(2)
        .map(|pair| edge(pair[0], pair[1]))
        .collect();
    edges.push(edge(agents[agents.len() - 1], "end"));
    TemplateFacts {
        graph_id: graph_id.to_string(),
        entry_revision: 1,
        definition_digest: digest_of(graph_id),
        shape: AgentGraphShape {
            entry_node: agents[0].to_string(),
            nodes,
            edges,
            max_iterations: 8,
        },
    }
}

fn reference(template: &TemplateFacts) -> AgentGraphEntryRef {
    AgentGraphEntryRef {
        tenant_id: TENANT.to_string(),
        graph_id: template.graph_id.clone(),
        entry_revision: template.entry_revision,
        definition_digest: template.definition_digest.clone(),
        actor_scope: "operator".to_string(),
        purpose_id: "agent-graph:publish".to_string(),
        policy_digest: digest_of("graph-policy"),
        composed_work_ceiling: 8,
    }
}

fn decide_over(templates: Vec<TemplateFacts>) -> super::super::Assembly {
    let mut asked = request(&["eg:task/research"], &[]);
    asked.templates = bounded(templates.iter().map(reference).collect());
    let inputs = build_inputs(
        asked,
        research_library(),
        templates,
        eg_types::decision::DecisionPolicy::engine_default(),
    )
    .expect("inputs");
    let assembly = assemble(inputs, identity()).expect("assembles");
    replay_check(&assembly.record).expect("replays");
    assembly
}

#[test]
fn a_two_slot_template_is_filled_with_one_agent_per_slot() {
    let assembly = decide_over(vec![template("pair", &["planner", "worker"])]);
    assert!(assembly.record.is_solved(), "{:?}", assembly.record.outcome);
    assert_eq!(assembly.agents.len(), 2);
    let graph = assembly.graph.expect("a graph");
    graph
        .validate()
        .expect("the filled template is a valid shape");
    let pinned: Vec<(&str, String)> = graph
        .shape
        .pinned_agents()
        .into_iter()
        .map(|(id, digest)| (id, digest.to_string()))
        .collect();
    for (agent, (id, digest)) in assembly.agents.iter().zip(&pinned) {
        assert_eq!(agent.agent_id, *id);
        assert_eq!(
            eg_types::agent_library::draft_definition_digest(agent),
            *digest
        );
    }
    let DecisionOutcome::Solved { slots, .. } = &assembly.record.outcome else {
        unreachable!()
    };
    assert!(slots.iter().any(|slot| slot.slot.starts_with("planner/")));
    assert!(slots.iter().any(|slot| slot.slot.starts_with("worker/")));
}

#[test]
fn the_cheaper_topology_wins_and_the_other_is_explained() {
    let assembly = decide_over(vec![
        template("pair", &["planner", "worker"]),
        template("solo", &["worker"]),
    ]);
    assert_eq!(assembly.agents.len(), 1, "one slot needs fewer components");
    let loser = assembly
        .record
        .why_not
        .iter()
        .find(|why| why.slot == "template")
        .expect("the losing template is explained");
    assert_eq!(loser.component_id, "pair");
    assert!(loser.forced_objective.is_some());
}

#[test]
fn a_template_that_is_not_the_named_revision_is_refused() {
    let solo = template("solo", &["worker"]);
    let mut asked = request(&["eg:task/research"], &[]);
    let mut named = reference(&solo);
    named.entry_revision = 2;
    asked.templates = bounded(vec![named]);
    let inputs = build_inputs(
        asked,
        research_library(),
        vec![solo],
        eg_types::decision::DecisionPolicy::engine_default(),
    )
    .expect("inputs");
    let error = assemble(inputs, identity()).expect_err("refused");
    assert_eq!(
        error.code,
        eg_types::decision::DecisionErrorCode::AssemblyInputsInvalid
    );
}
