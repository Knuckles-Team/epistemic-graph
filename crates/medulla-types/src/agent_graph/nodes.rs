//! Node/count validation, split out of `agent_graph.rs` (KISS file-budget) purely so
//! this logic doesn't grow the parent file past its aggregate line/function caps.
//! Behaviour is unchanged from when this was inline in
//! [`super::AgentGraphShape::validate`].

use super::*;

/// Node/edge counts and the iteration ceiling are in bounds.
pub(super) fn validate_counts(shape: &AgentGraphShape) -> Result<(), String> {
    if shape.nodes.is_empty() || shape.nodes.len() > MAX_NODES {
        return Err("agent graph has an invalid node count".to_string());
    }
    if shape.edges.len() > MAX_EDGES {
        return Err("agent graph has an invalid edge count".to_string());
    }
    if shape.max_iterations == 0 || shape.max_iterations > MAX_ITERATIONS_CEILING {
        return Err("agent graph max_iterations is out of range".to_string());
    }
    Ok(())
}

/// Every node is individually valid and no `node_id` repeats.
pub(super) fn index_nodes_by_id(
    shape: &AgentGraphShape,
) -> Result<BTreeMap<&str, &AgentGraphNode>, String> {
    let mut by_id: BTreeMap<&str, &AgentGraphNode> = BTreeMap::new();
    for node in &shape.nodes {
        validate_text("node_id", &node.node_id)?;
        node.validate()?;
        if by_id.insert(node.node_id.as_str(), node).is_some() {
            return Err(format!(
                "agent graph node '{}' is declared twice",
                node.node_id
            ));
        }
    }
    Ok(by_id)
}

pub(super) fn validate_entry_node(
    shape: &AgentGraphShape,
    by_id: &BTreeMap<&str, &AgentGraphNode>,
) -> Result<(), String> {
    if !by_id.contains_key(shape.entry_node.as_str()) {
        return Err(format!(
            "agent graph entry_node '{}' is not a declared node",
            shape.entry_node
        ));
    }
    Ok(())
}
