//! The topology half of input validation: a request may only tighten the
//! policy's caps, and what the topology question read is recorded exactly
//! once, consistently with the request (so replay reads the same facts).

use eg_types::decision::{
    DecisionErrorCode, DecisionInputs, TemplateFacts, TopologyInputs, TopologyPolicy,
    TopologyRequirements,
};

use super::super::AssembleError;

fn invalid(detail: impl Into<String>) -> AssembleError {
    AssembleError::new(DecisionErrorCode::AssemblyInputsInvalid, detail)
}

pub(in super::super) fn inputs(inputs: &DecisionInputs) -> Result<(), AssembleError> {
    let requirements = inputs.request.requirements.topology.as_ref();
    match (requirements, inputs.topology.as_ref()) {
        (None, None) => Ok(()),
        (Some(requirements), Some(read)) => check(inputs, requirements, read),
        (None, Some(_)) => Err(invalid("topology inputs without a topology request")),
        (Some(_), None) => Err(invalid("a topology request without its topology inputs")),
    }
}

fn check(
    inputs: &DecisionInputs,
    requirements: &TopologyRequirements,
    read: &TopologyInputs,
) -> Result<(), AssembleError> {
    let policy = inputs
        .policy
        .topology
        .clone()
        .unwrap_or_else(TopologyPolicy::engine_default);
    if let Some(field) = requirements.caps.loosened_field(&policy.caps) {
        return Err(AssembleError::new(
            DecisionErrorCode::PolicyLoosening,
            format!("the request widens the policy's topology {field}"),
        ));
    }
    if inputs.templates.is_empty() {
        return Err(invalid("a topology question is asked of graph templates"));
    }
    if requirements.per_agent_subtasks == 0 || requirements.task_classes.is_empty() {
        return Err(invalid(
            "a topology request names task classes and a positive per-agent load",
        ));
    }
    for template in inputs.templates.iter() {
        check_template(template)?;
    }
    check_read(requirements, read)
}

/// A template's topology facts are valid on their own and name only its own
/// `Agent` nodes.
fn check_template(template: &TemplateFacts) -> Result<(), AssembleError> {
    let Some(facts) = template.topology.as_ref() else {
        return Ok(());
    };
    facts.validate().map_err(invalid)?;
    let nodes: Vec<&str> = template
        .slot_nodes()
        .iter()
        .map(|node| node.node_id.as_str())
        .collect();
    match facts
        .slots
        .iter()
        .find(|slot| !nodes.contains(&slot.node_id.as_str()))
    {
        Some(slot) => Err(invalid(format!(
            "topology slot '{}' is not an agent node of '{}'",
            slot.node_id, template.graph_id
        ))),
        None => Ok(()),
    }
}

/// The headroom is one observation per requested cell, in request order, one
/// cell per resource class; admissibility facts are sorted and distinct.
fn check_read(
    requirements: &TopologyRequirements,
    read: &TopologyInputs,
) -> Result<(), AssembleError> {
    let cells: Vec<&str> = read.headroom.iter().map(|h| h.cell_id.as_str()).collect();
    let requested: Vec<&str> = requirements
        .capacity
        .cells
        .iter()
        .map(String::as_str)
        .collect();
    if cells != requested {
        return Err(invalid(
            "the headroom is not one observation per requested cell",
        ));
    }
    let mut classes: Vec<_> = read.headroom.iter().map(|h| h.class).collect();
    classes.sort();
    classes.dedup();
    if classes.len() != read.headroom.len() {
        return Err(invalid(
            "a topology request names one cell per resource class",
        ));
    }
    if read.admissions.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("admissibility facts must be sorted and distinct"));
    }
    if !read.schema_digest.starts_with("sha256:") {
        return Err(invalid("the schema digest is not a sha256 digest"));
    }
    Ok(())
}
