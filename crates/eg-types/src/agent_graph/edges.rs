//! Edge validation, split out of `agent_graph.rs` (KISS file-budget) purely so this
//! logic doesn't grow the parent file past its aggregate line/function caps. Behaviour
//! is unchanged from when this was inline in [`super::AgentGraphShape::validate`].

use super::*;

/// Every edge names declared nodes, is not repeated, never leaves an `End` node, and a
/// conditional edge leaves only a `Decision` node. Per-edge checks live in
/// `validate_one_edge` purely to keep this loop under the complexity cap.
pub(super) fn validate_edges(
    shape: &AgentGraphShape,
    by_id: &BTreeMap<&str, &AgentGraphNode>,
) -> Result<(), String> {
    let mut seen_edges = BTreeSet::new();
    for edge in &shape.edges {
        validate_one_edge(edge, by_id, &mut seen_edges)?;
    }
    Ok(())
}

/// One edge names declared nodes, is not a repeat of an earlier edge, does not leave
/// an `End` node, and -- if it carries a `condition` -- names a well-formed predicate
/// dependency and leaves only a `Decision` node (a conditional edge anywhere else has
/// nothing to branch on).
fn validate_one_edge<'a>(
    edge: &'a AgentGraphEdge,
    by_id: &BTreeMap<&str, &AgentGraphNode>,
    seen_edges: &mut BTreeSet<(&'a str, &'a str)>,
) -> Result<(), String> {
    let Some(from) = by_id.get(edge.from.as_str()) else {
        return Err(format!(
            "agent graph edge leaves undeclared node '{}'",
            edge.from
        ));
    };
    if !by_id.contains_key(edge.to.as_str()) {
        return Err(format!(
            "agent graph edge enters undeclared node '{}'",
            edge.to
        ));
    }
    if !seen_edges.insert((edge.from.as_str(), edge.to.as_str())) {
        return Err(format!(
            "agent graph declares edge '{}' -> '{}' twice",
            edge.from, edge.to
        ));
    }
    if matches!(from.kind, AgentGraphNodeKind::End) {
        return Err(format!(
            "agent graph end node '{}' cannot have an outgoing edge",
            edge.from
        ));
    }
    let Some(condition) = &edge.condition else {
        return Ok(());
    };
    validate_dependency("condition", condition, AgentComponentKind::Predicate)?;
    if !matches!(from.kind, AgentGraphNodeKind::Decision { .. }) {
        return Err(format!(
            "agent graph edge '{}' -> '{}' is conditional but leaves a {} node, \
             not a decision node",
            edge.from,
            edge.to,
            from.kind.label()
        ));
    }
    Ok(())
}
