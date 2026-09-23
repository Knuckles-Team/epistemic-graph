//! The one-agent graph rule (DECIDE-LAYER-DESIGN §4.6 step 1).
//!
//! A solved assembly is always answered as a runnable shape: one Agent Library
//! entry draft holding the selected components in their typed slots, and a
//! two-node graph (that agent, then `End`) whose agent node pins the entry by
//! its definition digest. Both are functions of the record's INPUTS alone --
//! never of the caller, the clock or the record digest -- so a replay rebuilds
//! them byte for byte, and the record's `graph_digest` is stable.
//!
//! Provenance is honest rather than invented: the agent's source revision IS
//! the decision's input set (`source_revision_digest = inputs_digest`), its
//! policy digest is the decision policy's, and its actor scope names the
//! engine's decision function rather than whoever asked.

use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
use eg_types::agent_graph::{
    AgentGraphDraft, AgentGraphEdge, AgentGraphNode, AgentGraphNodeKind, AgentGraphShape,
};
use eg_types::agent_library::{AgentLibraryEntryDraft, AgentRuntimeContract};
use eg_types::decision::{CandidateFacts, DecisionInputs, TemplateFacts};

use super::facts;

/// The actor scope every assembled draft is attributed to.
pub(super) const ASSEMBLY_ACTOR_SCOPE: &str = "eg:decide";
/// The purpose every assembled draft is published under.
pub(super) const ASSEMBLY_PURPOSE: &str = "agent-assemble";
const AGENT_NODE: &str = "agent";
const END_NODE: &str = "end";

/// The slot a selected candidate fills, by kind.
pub(super) fn slot_name(kind: AgentComponentKind) -> &'static str {
    kind.as_str()
}

fn dependency(candidate: &CandidateFacts) -> ComponentDependency {
    ComponentDependency {
        component_id: candidate.component_id.clone(),
        kind: candidate.kind,
        definition_digest: candidate.definition_digest.clone(),
    }
}

fn of_kind<'a>(
    selected: &[&'a CandidateFacts],
    kind: AgentComponentKind,
) -> Vec<&'a CandidateFacts> {
    selected
        .iter()
        .copied()
        .filter(|candidate| candidate.kind == kind)
        .collect()
}

fn dependencies(
    selected: &[&CandidateFacts],
    kind: AgentComponentKind,
) -> Vec<ComponentDependency> {
    of_kind(selected, kind)
        .into_iter()
        .map(dependency)
        .collect()
}

/// The assembled agents and the graph that runs them, validated; or the
/// validation failure the no-good loop cuts on.
///
/// With no template the graph is the one-agent graph. With a template, slot
/// `j` is the template's `j`-th `Agent` node: that node is re-pointed at the
/// agent assembled for it and inherits nothing else, and every other node of
/// the template is kept exactly as published.
pub(super) fn drafts(
    inputs: &DecisionInputs,
    inputs_digest: &str,
    template: Option<&TemplateFacts>,
    chosen: &[(usize, &CandidateFacts)],
) -> Result<(Vec<AgentLibraryEntryDraft>, AgentGraphDraft), String> {
    let nodes: Vec<Option<&AgentGraphNode>> = match template {
        Some(template) => template.slot_nodes().into_iter().map(Some).collect(),
        None => vec![None],
    };
    let mut agents = Vec::with_capacity(nodes.len());
    for (slot, node) in nodes.iter().enumerate() {
        let selected: Vec<&CandidateFacts> = chosen
            .iter()
            .filter(|(placed, _)| *placed == slot)
            .map(|(_, candidate)| *candidate)
            .collect();
        let naming = SlotNaming {
            inputs_digest,
            slot: template.map(|_| slot),
            node: *node,
        };
        let agent = agent_draft(inputs, &naming, &selected)?;
        agent.validate()?;
        agents.push(agent);
    }
    let graph = match template {
        Some(template) => template_graph(inputs, inputs_digest, template, &agents),
        None => graph_draft(inputs, inputs_digest, &agents[0]),
    };
    graph.validate()?;
    Ok((agents, graph))
}

/// How one slot's agent is named and what node contracts it inherits.
struct SlotNaming<'a> {
    inputs_digest: &'a str,
    /// `Some` inside a template: the agent id gains the slot suffix.
    slot: Option<usize>,
    node: Option<&'a AgentGraphNode>,
}

fn single<'a>(
    selected: &[&'a CandidateFacts],
    kind: AgentComponentKind,
) -> Result<&'a CandidateFacts, String> {
    match of_kind(selected, kind).as_slice() {
        [one] => Ok(one),
        other => Err(format!(
            "ONE_AGENT_SLOT: an agent needs exactly one {}, the selection holds {}",
            kind.as_str(),
            other.len()
        )),
    }
}

/// The short, stable name both drafts share: the first 16 hex digits of the
/// inputs digest.
fn assembled_id(inputs_digest: &str) -> String {
    let hex = inputs_digest
        .strip_prefix("sha256:")
        .unwrap_or(inputs_digest);
    format!("assembled-{}", &hex[..hex.len().min(16)])
}

fn agent_draft(
    inputs: &DecisionInputs,
    naming: &SlotNaming<'_>,
    selected: &[&CandidateFacts],
) -> Result<AgentLibraryEntryDraft, String> {
    let inputs_digest = naming.inputs_digest;
    let agent_id = match naming.slot {
        Some(slot) => format!("{}-s{slot}", assembled_id(inputs_digest)),
        None => assembled_id(inputs_digest),
    };
    let model = single(selected, AgentComponentKind::ModelProfile)?;
    let prompt = single(selected, AgentComponentKind::SystemPrompt)?;
    let model_identity = facts::model_facts(model)
        .map(|facts| facts.model_identity.to_string())
        .ok_or_else(|| "ONE_AGENT_SLOT: the model profile carries no model facts".to_string())?;
    Ok(AgentLibraryEntryDraft {
        agent_id,
        package_id: "eg-decide".to_string(),
        version: "1".to_string(),
        role: "assembled".to_string(),
        role_digest: inputs_digest.to_string(),
        system_prompt: dependency(prompt),
        tools: dependencies(selected, AgentComponentKind::Tool),
        skills: dependencies(selected, AgentComponentKind::Skill),
        model_profile: dependency(model),
        model_identity,
        ontologies: dependencies(selected, AgentComponentKind::Ontology),
        tenant_id: inputs.request.tenant_id.clone(),
        actor_scope: ASSEMBLY_ACTOR_SCOPE.to_string(),
        purpose_id: ASSEMBLY_PURPOSE.to_string(),
        policy_digest: inputs.policy_digest.clone(),
        source_revision: "decision-inputs".to_string(),
        source_revision_digest: inputs_digest.to_string(),
        runtime: AgentRuntimeContract {
            toolset_refs: dependencies(selected, AgentComponentKind::Toolset),
            deps_contract: naming.node.and_then(|node| node.deps_contract.clone()),
            output_contract: naming.node.and_then(|node| node.output_contract.clone()),
            ..AgentRuntimeContract::default()
        },
        instantiated_from: None,
    })
}

fn graph_draft(
    inputs: &DecisionInputs,
    inputs_digest: &str,
    agent: &AgentLibraryEntryDraft,
) -> AgentGraphDraft {
    let agent_node = AgentGraphNode {
        node_id: AGENT_NODE.to_string(),
        kind: AgentGraphNodeKind::Agent {
            agent_id: agent.agent_id.clone(),
            definition_digest: eg_types::agent_library::draft_definition_digest(agent),
        },
        deps_contract: None,
        output_contract: None,
    };
    let end_node = AgentGraphNode {
        node_id: END_NODE.to_string(),
        kind: AgentGraphNodeKind::End,
        deps_contract: None,
        output_contract: None,
    };
    AgentGraphDraft {
        graph_id: assembled_id(inputs_digest),
        version: "1".to_string(),
        shape: AgentGraphShape {
            entry_node: AGENT_NODE.to_string(),
            nodes: vec![agent_node, end_node],
            edges: vec![AgentGraphEdge {
                from: AGENT_NODE.to_string(),
                to: END_NODE.to_string(),
                condition: None,
            }],
            max_iterations: 2,
        },
        tenant_id: inputs.request.tenant_id.clone(),
        actor_scope: ASSEMBLY_ACTOR_SCOPE.to_string(),
        purpose_id: ASSEMBLY_PURPOSE.to_string(),
        policy_digest: inputs.policy_digest.clone(),
        synthesis_evidence: None,
    }
}

/// The template's shape with each slot node pointed at its assembled agent.
fn template_graph(
    inputs: &DecisionInputs,
    inputs_digest: &str,
    template: &TemplateFacts,
    agents: &[AgentLibraryEntryDraft],
) -> AgentGraphDraft {
    let mut shape = template.shape.clone();
    let mut slot = 0usize;
    for node in shape.nodes.iter_mut() {
        if node.kind.agent_pin().is_none() {
            continue;
        }
        if let Some(agent) = agents.get(slot) {
            node.kind = AgentGraphNodeKind::Agent {
                agent_id: agent.agent_id.clone(),
                definition_digest: eg_types::agent_library::draft_definition_digest(agent),
            };
        }
        slot += 1;
    }
    AgentGraphDraft {
        graph_id: assembled_id(inputs_digest),
        version: "1".to_string(),
        shape,
        tenant_id: inputs.request.tenant_id.clone(),
        actor_scope: ASSEMBLY_ACTOR_SCOPE.to_string(),
        purpose_id: ASSEMBLY_PURPOSE.to_string(),
        policy_digest: inputs.policy_digest.clone(),
        synthesis_evidence: None,
    }
}
