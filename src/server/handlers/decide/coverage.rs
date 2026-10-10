//! `Method::CapabilityCoverage` (EG-DECISION-ENGINE-R126.2.1): which visible
//! components cover each capability a task needs.
//!
//! Reads the same tenant-bound Agent Library snapshot `AgentAssemble` reads
//! (the caller's candidate scope, plus every registered `A2aAgentCard`
//! component) and hands it to the pure
//! `eg_types::decision::derivation::capability_coverage_query`. No model, no
//! ranking, no commit.

use super::SharedState;
use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use eg_types::decision::coverage::CapabilityCoverageRequest;

/// Resolve the tenant's visible candidates and A2A cards and answer their
/// per-capability coverage of the request's closure.
#[cfg(feature = "decide")]
pub(crate) async fn handle_capability_coverage(
    state: &SharedState,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: CapabilityCoverageRequest,
) -> Response {
    match coverage_for(state, verified, &request).await {
        Ok(result) => Response::ok(
            req_id,
            crate::protocol::ResultPayload::of_ref::<
                eg_types::result_contract::storage::CapabilityCoverage,
            >(&result),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "decide")]
async fn coverage_for(
    state: &SharedState,
    verified: &VerifiedRequestContext,
    request: &CapabilityCoverageRequest,
) -> Result<eg_types::decision::coverage::CapabilityCoverageResult, String> {
    use eg_types::agent_component::AgentComponentKind;
    use eg_types::decision::coverage::{
        CapabilityCoverageOutcome, CapabilityCoverageResult,
        CAPABILITY_COVERAGE_RESULT_SCHEMA_VERSION,
    };
    use eg_types::decision::derivation::capability_coverage_query;
    use eg_types::decision::LibraryCandidateScope;

    if request.tenant_id != verified.tenant() {
        return Err("ACCESS_DENIED: coverage tenant must match the verified request tenant".into());
    }
    let store = state.write().await.ensure_agent_library()?;
    let library: Vec<_> = store
        .assembly_candidates(&request.tenant_id, &request.candidates)?
        .into_iter()
        .filter(|entry| entry.kind != AgentComponentKind::A2aAgentCard)
        .collect();
    let candidates = crate::server::persistence::decision_record::candidate_facts(&library)?;
    let card_scope = LibraryCandidateScope {
        kinds: eg_types::contract::BoundedVec::new(vec![AgentComponentKind::A2aAgentCard])
            .map_err(|error| error.to_string())?,
        classification_under: None,
    };
    let a2a_cards: Vec<(String, Vec<String>)> = store
        .assembly_candidates(&request.tenant_id, &card_scope)?
        .into_iter()
        .map(|entry| (entry.component_id, entry.declared_capabilities))
        .collect();
    let ontology_digest = eg_types::agent_ontology::ontology_digest();
    let coverage = match capability_coverage_query(
        &request.requirements,
        &ontology_digest,
        &candidates,
        &a2a_cards,
    ) {
        Ok(capabilities) => CapabilityCoverageOutcome::Covered { capabilities },
        Err(reasons) => CapabilityCoverageOutcome::Abstained { reasons },
    };
    Ok(CapabilityCoverageResult {
        schema_version: CAPABILITY_COVERAGE_RESULT_SCHEMA_VERSION,
        ontology_digest,
        coverage,
    })
}

/// A build without the Decide layer still serves the method: it refuses by
/// name.
#[cfg(not(feature = "decide"))]
pub(crate) async fn handle_capability_coverage(
    _state: &SharedState,
    req_id: u64,
    _verified: &VerifiedRequestContext,
    _request: CapabilityCoverageRequest,
) -> Response {
    Response::err(req_id, "CapabilityCoverage requires the `decide` feature")
}
