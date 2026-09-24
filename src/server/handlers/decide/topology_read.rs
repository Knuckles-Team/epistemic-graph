//! What a swarm-topology question reads beside the agent library: the request
//! graph's schema entailments (ST-5) and its capacity headroom (ST-8), both
//! recorded in the decision inputs so the pure decision function -- and every
//! replay of the record -- reads the same facts.

use std::sync::Arc;

use eg_types::contract::BoundedVec;
use eg_types::decision::{AssemblyRequest, TemplateFacts, TopologyInputs};

use super::{capacity_premise, topology_schema, SharedState};
use crate::server::auth::VerifiedRequestContext;

/// The request graph's core, behind the ordinary read ACL.
async fn graph_core(
    state: &SharedState,
    graph: &str,
    verified: &VerifiedRequestContext,
) -> Result<Arc<crate::graph::GraphCore>, String> {
    let s = state.read().await;
    let entry = s
        .registry
        .get(graph)
        .ok_or_else(|| format!("Graph '{graph}' not found"))?;
    crate::server::access::check_graph_access(
        &s.isolation,
        Some(verified.agent_id()),
        graph,
        entry.graph_type,
        entry.owner.as_deref(),
        crate::isolation::AccessLevel::Read,
    )?;
    Ok(entry.core.clone())
}

fn bounded<T, const N: usize>(values: Vec<T>, what: &str) -> Result<BoundedVec<T, N>, String> {
    BoundedVec::new(values).map_err(|error| format!("ASSEMBLY_INPUTS_INVALID: {what}: {error}"))
}

/// The topology inputs of `request` over `templates`, or `None` for a plain
/// assembly.
pub(super) async fn read(
    state: &SharedState,
    graph: &str,
    verified: &VerifiedRequestContext,
    request: &AssemblyRequest,
    templates: &[TemplateFacts],
) -> Result<Option<TopologyInputs>, String> {
    let Some(asked) = request.requirements.topology.as_ref() else {
        return Ok(None);
    };
    let core = graph_core(state, graph, verified).await?;
    let tasks: Vec<String> = asked.task_classes.iter().cloned().collect();
    let classes: Vec<String> = templates
        .iter()
        .filter_map(|template| template.topology.as_ref())
        .map(|facts| facts.class_iri.clone())
        .collect();
    // In-process OWL completion over the composed schema: CPU work.
    let entailed =
        tokio::task::spawn_blocking(move || topology_schema::entail(&core, &tasks, &classes))
            .await
            .map_err(|error| format!("topology entailment task failed: {error}"))??;
    let headroom = capacity_premise::headroom(state, graph, &asked.capacity).await?;
    Ok(Some(TopologyInputs {
        schema_digest: entailed.schema_digest,
        admissions: bounded(entailed.admissions, "admissions")?,
        verify_required_by: bounded(entailed.verify_required_by, "verify_required_by")?,
        headroom: bounded(headroom, "headroom")?,
    }))
}
