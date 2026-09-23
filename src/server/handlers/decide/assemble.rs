//! `Method::AgentAssemble`: prove an agent graph out of one tenant's library.
//!
//! Reads the tenant-bound candidate scope from one Agent Library snapshot
//! (step 1a), hands the complete inputs to the pure decision function
//! (`eg_compute::assemble`), and answers the sealed record plus -- only when
//! it is `Solved` -- the one-agent graph, the agent it runs and the model the
//! certificate covers. Nothing here commits; the record becomes durable only
//! through `DecisionCommit`.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;
use eg_types::decision::AssemblyRequest;

/// The audit target every Decide-layer decision line is written under.
#[cfg(feature = "decide")]
pub(crate) const DECIDE_AUDIT_TARGET: &str = "eg::decide::audit";

/// Read the candidate scope, solve, and answer a record plus -- only when the
/// outcome is `Solved` -- the graph draft it proves.
#[cfg(feature = "decide")]
pub(crate) async fn handle_agent_assemble(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: AssemblyRequest,
) -> Response {
    match assemble_for(state, verified, request).await {
        Ok(result) => {
            audit_line("agent-assemble", &result.record, verified);
            Response::ok(
                req_id,
                crate::protocol::ResultPayload::of_ref::<
                    eg_types::result_contract::storage::AgentAssemble,
                >(&result),
            )
        }
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "decide")]
async fn assemble_for(
    state: &Arc<RwLock<ServerState>>,
    verified: &VerifiedRequestContext,
    request: AssemblyRequest,
) -> Result<eg_types::decision::AssemblyResult, String> {
    use eg_compute::assemble::{assemble, inputs, RecordIdentity};

    if request.tenant_id != verified.tenant() {
        return Err("ACCESS_DENIED: assembly tenant must match the verified request tenant".into());
    }
    let store = state.write().await.ensure_agent_library()?;
    let policy = resolve_policy(&store, &request)?;
    let entries = store.assembly_candidates(&request.tenant_id, &request.candidates)?;
    let candidates = crate::server::persistence::decision_record::candidate_facts(&entries)?;
    let templates = request
        .templates
        .iter()
        .map(|reference| store.assembly_template(&request.tenant_id, reference))
        .collect::<Result<Vec<_>, String>>()?;
    let inputs =
        inputs(request, candidates, templates, policy).map_err(|error| error.to_string())?;
    let identity = RecordIdentity {
        tenant_id: verified.tenant().to_string(),
        caller_principal: verified.principal_persistence_id(),
        created_at_ms: crate::server::dispatch::authoritative_now_ms(),
    };
    // Bounded by the node budget, but CPU work all the same.
    let assembly = tokio::task::spawn_blocking(move || assemble(inputs, identity))
        .await
        .map_err(|error| format!("assembly task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    Ok(eg_types::decision::AssemblyResult {
        schema_version: eg_types::decision::ASSEMBLY_RESULT_SCHEMA_VERSION,
        record: assembly.record,
        graph: assembly.graph,
        agents: eg_types::contract::BoundedVec::new(assembly.agents)
            .map_err(|error| error.to_string())?,
        model: assembly.model,
    })
}

/// The policy a request decides under: the engine default, or the verified
/// body of the published `DecisionPolicy` component revision it pins.
#[cfg(feature = "decide")]
fn resolve_policy(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    request: &AssemblyRequest,
) -> Result<eg_types::decision::DecisionPolicy, String> {
    use eg_types::decision::{DecisionPolicy, DecisionPolicyRef};
    match &request.policy {
        DecisionPolicyRef::Default => Ok(DecisionPolicy::engine_default()),
        DecisionPolicyRef::Pinned { component } => {
            store.pinned_decision_policy(&request.tenant_id, component)
        }
    }
}

/// One structured audit line per decision: who asked, what was decided, and
/// how far it can be trusted. Record ids only -- never inputs or text.
#[cfg(feature = "decide")]
pub(crate) fn audit_line(
    action: &'static str,
    record: &eg_types::decision::DecisionRecord,
    verified: &VerifiedRequestContext,
) {
    let outcome = match &record.outcome {
        eg_types::decision::DecisionOutcome::Solved { .. } => "solved",
        eg_types::decision::DecisionOutcome::Abstained { .. } => "abstained",
    };
    tracing::info!(
        target: DECIDE_AUDIT_TARGET,
        action,
        record_id = %record.record_id,
        tenant = %verified.tenant(),
        caller = %verified.principal_persistence_id(),
        outcome,
        resolution_kind = ?record.resolution_kind,
        evidence_class = ?record.evidence_class,
        eliminated = record.eliminated.len(),
        catalog_digest = %record.inputs.catalog_digest,
        "decide-layer decision"
    );
}

/// A build without the Decide layer still serves the method: it refuses by
/// name.
#[cfg(not(feature = "decide"))]
pub(crate) async fn handle_agent_assemble(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _verified: &VerifiedRequestContext,
    _request: AssemblyRequest,
) -> Response {
    Response::err(req_id, "AgentAssemble requires the `decide` feature")
}
