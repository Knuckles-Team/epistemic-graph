//! Authenticated Agent Library operation handlers.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::{Response, ResultPayload};
use crate::server::state::ServerState;
use eg_types::contract::Nonce;

#[cfg(feature = "redb")]
pub(crate) fn bind_agent_library_context(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut context: eg_types::AgentLibraryMutationContext,
    purpose_id: &str,
    requires_nonce: bool,
) -> Result<eg_types::AgentLibraryMutationContext, String> {
    if context.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: Agent Library tenant must match verified request tenant".to_string(),
        );
    }
    let policy_revision = verified.claims().policy_version.clone();
    let caller = verified.principal_persistence_id();
    let nonce = match verified.attempt_nonce() {
        Some(nonce) => nonce,
        None if requires_nonce => {
            return Err("Agent Library writes require an authenticated attempt nonce".to_string())
        }
        // Status is a read-only query and never reaches the mutation kernel;
        // an internal query producer may still provide an explicit ephemeral
        // nonce to satisfy the shared context shape without consuming it.
        None => Nonce::minted(),
    };
    context.request_id = req_id;
    context.principal = store.owner_principal().to_string();
    context.caller_principal = caller.clone();
    context.attempt_nonce = nonce;
    context.tenant_id = verified.tenant().to_string();
    context.actor_scope = caller;
    // A string, not a record-typed enum: this owner carries agent entries AND
    // agent graphs (RF-ADR-008), and the purpose is the only thing that varies.
    context.purpose_id = purpose_id.to_string();
    context.policy_revision = policy_revision;
    context.policy_digest =
        crate::server::persistence::agent_library::current_agent_library_policy_digest()?;
    // This identifier is derived from the admitted policy revision, rather
    // than copied from a request body. It stays stable across transport retries
    // so it cannot turn a byte-identical operation into a false conflict.
    context.policy_decision_id = format!("agent-library:policy:{}", context.policy_revision);
    context.idempotency_key = verified.idempotency_key().to_string();
    context.trace_id = None;
    context.created_at_ms = crate::server::dispatch::authoritative_now_ms();
    Ok(context)
}

/// Bind the context of a read-only Agent Library status query. Status never
/// reaches the mutation kernel, so no attempt nonce is required; a binding
/// failure is answered as this request's error response.
#[cfg(feature = "redb")]
fn bind_status_context(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    context: eg_types::AgentLibraryMutationContext,
    purpose_id: &str,
) -> Result<eg_types::AgentLibraryMutationContext, Response> {
    bind_agent_library_context(store, req_id, verified, context, purpose_id, false)
        .map_err(|error| Response::err(req_id, error))
}

#[cfg(feature = "redb")]
pub(crate) fn bind_agent_library_draft(
    draft: eg_types::AgentLibraryEntryDraft,
    verified: &crate::server::auth::VerifiedRequestContext,
    _action_context: &eg_types::AgentLibraryMutationContext,
) -> Result<eg_types::AgentLibraryEntryDraft, String> {
    if draft.tenant_id != verified.tenant() {
        return Err(
            "ACCESS_DENIED: Agent Library definition tenant must match verified request tenant"
                .to_string(),
        );
    }
    // `draft.agent_id` names the selected built definition. Its actor scope,
    // purpose, and policy digest are immutable definition provenance and must
    // survive a later action by another caller under a newer policy. The
    // authenticated request's action authority is carried separately in the
    // mutation context and outbox event; body attribution never authorizes the
    // action or replaces historical definition fields.
    Ok(draft)
}

#[cfg(feature = "redb")]
async fn ensure_agent_library(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
) -> Result<Arc<crate::server::persistence::agent_library::AgentLibraryStore>, Response> {
    let mut guard = state.write().await;
    guard
        .ensure_agent_library()
        .map_err(|error| Response::err(req_id, error))
}

#[cfg(feature = "redb")]
fn validate_component_op(
    op: &eg_types::agent_component::AgentComponentOp,
    verified: &crate::server::auth::VerifiedRequestContext,
) -> Result<(), String> {
    op.validate()?;
    if op.tenant_id() != verified.tenant() {
        return Err(
            "ACCESS_DENIED: agent component tenant must match verified request tenant".to_string(),
        );
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn validate_template_op(
    op: &eg_types::agent_template::AgentTemplateOp,
    verified: &crate::server::auth::VerifiedRequestContext,
) -> Result<(), String> {
    op.validate()?;
    if op.tenant_id() != verified.tenant() {
        return Err(
            "ACCESS_DENIED: agent template tenant must match verified request tenant".to_string(),
        );
    }
    Ok(())
}

/// Serve the typed RF-020 Agent Library operations from the same authenticated
/// dispatch boundary as every other self-routing control-plane method.
#[cfg(feature = "redb")]
/// Publish, retire, inspect or SEARCH one durable agent component
/// (RF-ADR-008 layer 1).
///
/// Same store as the two layers above it. `Search` is the capability query the
/// layer exists for -- it resolves a task through EG's native ontology and
/// matches components by subsumption, so the answer comes from the graph rather
/// than from a model.
pub(crate) async fn handle_agent_component(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::agent_component::AgentComponentOp,
) -> Response {
    use eg_types::agent_component::AgentComponentOp;

    if let Err(error) = validate_component_op(&op, verified) {
        return Response::err(req_id, error);
    }
    let store = match ensure_agent_library(state, req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    match op {
        AgentComponentOp::Publish { request } => {
            component_publish(&store, req_id, verified, request)
        }
        AgentComponentOp::Retire { request } => component_retire(&store, req_id, verified, request),
        AgentComponentOp::Current {
            tenant_id,
            component_id,
        } => component_current(&store, req_id, tenant_id, component_id),
        AgentComponentOp::History {
            tenant_id,
            component_id,
        } => component_history(&store, req_id, tenant_id, component_id),
        AgentComponentOp::Status { request } => component_status(&store, req_id, verified, request),
        AgentComponentOp::Search { request } => component_search(&store, req_id, request),
        AgentComponentOp::Content { request } => {
            super::component_content::handle_component_content(&store, req_id, verified, request)
        }
    }
}

#[cfg(feature = "redb")]
fn component_publish(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: Box<eg_types::agent_component::AgentComponentPublishRequest>,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-component:publish",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    request.component.tenant_id = context.tenant_id.clone();
    request.component.actor_scope = context.actor_scope.clone();
    request.component.purpose_id = context.purpose_id.clone();
    request.component.policy_digest = context.policy_digest.clone();
    request.context = context;
    match store.publish_component(*request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentPublish>(
                &result.result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn component_retire(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::agent_component::AgentComponentRetireRequest,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-component:retire",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    request.context = context;
    match store.retire_component(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentRetire>(
                &result.result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn component_current(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    tenant_id: String,
    component_id: String,
) -> Response {
    match store.current_component(&tenant_id, &component_id) {
        Ok(entry) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentCurrent>(
                &entry,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn component_history(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    tenant_id: String,
    component_id: String,
) -> Response {
    match store.component_revisions(&tenant_id, &component_id) {
        Ok(entries) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentHistory>(
                &entries,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// The `bind_agent_library_context` purpose for a status read, named by the
/// mutation kind it is reporting on. A pack member's withdrawal and its
/// return are importer actions, not caller ones; the status read still has
/// to name them so a new kind cannot inherit an unrelated purpose.
///
/// A free function rather than a match inline in `component_status`: the
/// kind enum is exhaustive by design (RF-ADR-010), and every variant this
/// module adds would otherwise read as `component_status` itself getting
/// more complex, when the dispatch is the whole story.
#[cfg(feature = "redb")]
fn component_status_purpose(
    kind: eg_types::agent_component::AgentComponentMutationKind,
) -> &'static str {
    match kind {
        eg_types::agent_component::AgentComponentMutationKind::Publish => "agent-component:publish",
        eg_types::agent_component::AgentComponentMutationKind::Retire => "agent-component:retire",
        eg_types::agent_component::AgentComponentMutationKind::Withdraw => {
            "agent-component:withdraw"
        }
        eg_types::agent_component::AgentComponentMutationKind::Republish => {
            "agent-component:republish"
        }
    }
}

#[cfg(feature = "redb")]
fn component_status(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::agent_component::AgentComponentStatusRequest,
) -> Response {
    let purpose = component_status_purpose(request.kind);
    request.context = match bind_status_context(store, req_id, verified, request.context, purpose) {
        Ok(context) => context,
        Err(response) => return response,
    };
    match store.component_status(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentStatus>(
                &result.map(|result| result.result),
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn component_search(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    request: eg_types::agent_component::AgentComponentSearchRequest,
) -> Response {
    match store.search_components(&request) {
        Ok(page) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentComponentSearch>(
                &page,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// Publish, retire, inspect or INSTANTIATE one durable agent template
/// (RF-ADR-008 item C).
///
/// Same store as the three layers beside it. `Instantiate` is the operation
/// the layer exists for, and it is a READ: it binds parameters and returns an
/// ordinary `AgentLibraryEntryDraft`, which the caller then publishes through
/// `Method::AgentLibrary`. Nothing here commits an instance, so the library
/// write privilege is still what admits one.
#[cfg(feature = "redb")]
pub(crate) async fn handle_agent_template(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::agent_template::AgentTemplateOp,
) -> Response {
    use eg_types::agent_template::AgentTemplateOp;

    if let Err(error) = validate_template_op(&op, verified) {
        return Response::err(req_id, error);
    }
    let store = match ensure_agent_library(state, req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    match op {
        AgentTemplateOp::Publish { request } => template_publish(&store, req_id, verified, request),
        AgentTemplateOp::Retire { request } => template_retire(&store, req_id, verified, request),
        AgentTemplateOp::Current {
            tenant_id,
            template_id,
        } => template_current(&store, req_id, tenant_id, template_id),
        AgentTemplateOp::History {
            tenant_id,
            template_id,
        } => template_history(&store, req_id, tenant_id, template_id),
        AgentTemplateOp::Status { request } => template_status(&store, req_id, verified, request),
        AgentTemplateOp::Instantiate { request } => template_instantiate(&store, req_id, request),
    }
}

#[cfg(feature = "redb")]
fn template_publish(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: Box<eg_types::agent_template::AgentTemplatePublishRequest>,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-template:publish",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    // Definition provenance is bound to the AUTHENTICATED caller, not
    // to whatever the body claimed -- the same rule the three layers
    // beside it apply. The base's own tenant has to move with it or
    // `AgentTemplateDraft::validate` refuses the pair.
    request.template.tenant_id = context.tenant_id.clone();
    request.template.base.tenant_id = context.tenant_id.clone();
    request.template.actor_scope = context.actor_scope.clone();
    request.template.purpose_id = context.purpose_id.clone();
    request.template.policy_digest = context.policy_digest.clone();
    request.context = context;
    match store.publish_template(*request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentTemplatePublish>(
                &result.result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn template_retire(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::agent_template::AgentTemplateRetireRequest,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-template:retire",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    request.context = context;
    match store.retire_template(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentTemplateRetire>(
                &result.result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn template_current(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    tenant_id: String,
    template_id: String,
) -> Response {
    match store.current_template(&tenant_id, &template_id) {
        Ok(entry) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentTemplateCurrent>(
                &entry,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn template_history(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    tenant_id: String,
    template_id: String,
) -> Response {
    match store.template_revisions(&tenant_id, &template_id) {
        Ok(entries) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentTemplateHistory>(
                &entries,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn template_status(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::agent_template::AgentTemplateStatusRequest,
) -> Response {
    let purpose = match request.kind {
        eg_types::agent_template::AgentTemplateMutationKind::Publish => "agent-template:publish",
        eg_types::agent_template::AgentTemplateMutationKind::Retire => "agent-template:retire",
    };
    request.context = match bind_status_context(store, req_id, verified, request.context, purpose) {
        Ok(context) => context,
        Err(response) => return response,
    };
    match store.template_status(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentTemplateStatus>(
                &result.map(|result| result.result),
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn template_instantiate(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    request: eg_types::agent_template::AgentTemplateInstantiateRequest,
) -> Response {
    match store.instantiate_template(&request) {
        Ok(draft) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentTemplateInstantiate>(
                &draft,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// Publish, retire, or inspect one durable agent GRAPH (RF-ADR-008).
///
/// Routed to the SAME store as [`handle_agent_library`]: a graph is published
/// into the agent-library owner alongside the entries it composes, so there is
/// one `ensure_agent_library` and one physical authority (RF-RULING-004).
pub(crate) async fn handle_agent_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::agent_graph::AgentGraphOp,
) -> Response {
    use eg_types::agent_graph::AgentGraphOp;

    if let Err(error) = op.validate() {
        return Response::err(req_id, error);
    }
    let store = match ensure_agent_library(state, req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    match op {
        AgentGraphOp::Publish { request } => graph_publish(&store, req_id, verified, request),
        AgentGraphOp::Retire { request } => graph_retire(&store, req_id, verified, request),
        AgentGraphOp::Current {
            tenant_id,
            graph_id,
        } => graph_current(&store, req_id, verified, tenant_id, graph_id),
        AgentGraphOp::History {
            tenant_id,
            graph_id,
        } => graph_history(&store, req_id, verified, tenant_id, graph_id),
        AgentGraphOp::Status { request } => graph_status(&store, req_id, verified, request),
    }
}

#[cfg(feature = "redb")]
fn graph_publish(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: Box<eg_types::agent_graph::AgentGraphPublishRequest>,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-graph:publish",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    // The verified caller owns the tenant/actor/purpose on the record,
    // not the request body: a caller must not be able to publish a
    // graph attributed to another tenant.
    request.graph.tenant_id = context.tenant_id.clone();
    request.graph.actor_scope = context.actor_scope.clone();
    request.graph.purpose_id = context.purpose_id.clone();
    request.graph.policy_digest = context.policy_digest.clone();
    request.context = context;
    match store.publish_graph(*request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentGraphPublish>(
                &result.result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn graph_retire(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::agent_graph::AgentGraphRetireRequest,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-graph:retire",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    request.context = context;
    match store.retire_graph(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentGraphRetire>(
                &result.result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn graph_current(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    tenant_id: String,
    graph_id: String,
) -> Response {
    if tenant_id != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: agent graph tenant must match verified request tenant",
        );
    }
    match store.current_graph(&tenant_id, &graph_id) {
        Ok(entry) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentGraphCurrent>(&entry),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn graph_history(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    tenant_id: String,
    graph_id: String,
) -> Response {
    if tenant_id != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: agent graph tenant must match verified request tenant",
        );
    }
    match store.graph_revisions(&tenant_id, &graph_id) {
        Ok(entries) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentGraphHistory>(
                &entries,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn graph_status(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::agent_graph::AgentGraphStatusRequest,
) -> Response {
    let purpose = match request.kind {
        eg_types::agent_graph::AgentGraphMutationKind::Publish => "agent-graph:publish",
        eg_types::agent_graph::AgentGraphMutationKind::Retire => "agent-graph:retire",
    };
    request.context = match bind_status_context(store, req_id, verified, request.context, purpose) {
        Ok(context) => context,
        Err(response) => return response,
    };
    match store.graph_status(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentGraphStatus>(
                &result.map(|result| result.result),
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

pub(crate) async fn handle_agent_library(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    op: eg_types::AgentLibraryOp,
) -> Response {
    let store = match ensure_agent_library(state, req_id).await {
        Ok(store) => store,
        Err(response) => return response,
    };
    match op {
        eg_types::AgentLibraryOp::Publish { request } => {
            library_publish(&store, req_id, verified, request)
        }
        eg_types::AgentLibraryOp::Retire { request } => {
            library_retire(&store, req_id, verified, request)
        }
        eg_types::AgentLibraryOp::Current {
            tenant_id,
            agent_id,
        } => library_current(&store, req_id, verified, tenant_id, agent_id),
        eg_types::AgentLibraryOp::History {
            tenant_id,
            agent_id,
        } => library_history(&store, req_id, verified, tenant_id, agent_id),
        eg_types::AgentLibraryOp::Status { request } => {
            library_status(&store, req_id, verified, request)
        }
    }
}

#[cfg(feature = "redb")]
fn library_publish(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: Box<eg_types::AgentLibraryPublishRequest>,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-library:publish",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    request.context = context.clone();
    request.entry = match bind_agent_library_draft(request.entry, verified, &context) {
        Ok(entry) => entry,
        Err(error) => return Response::err(req_id, error),
    };
    match store.publish(*request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentLibraryPublish>(
                &result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn library_retire(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::AgentLibraryRetireRequest,
) -> Response {
    let context = match bind_agent_library_context(
        store,
        req_id,
        verified,
        request.context,
        "agent-library:retire",
        true,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    request.context = context;
    match store.retire(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentLibraryRetire>(
                &result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn library_current(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    tenant_id: String,
    agent_id: String,
) -> Response {
    if tenant_id != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: Agent Library tenant must match verified request tenant",
        );
    }
    match store.current(&tenant_id, &agent_id) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentLibraryCurrent>(
                &result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn library_history(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    tenant_id: String,
    agent_id: String,
) -> Response {
    if tenant_id != verified.tenant() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: Agent Library tenant must match verified request tenant",
        );
    }
    match store.revisions(&tenant_id, &agent_id) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentLibraryHistory>(
                &result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(feature = "redb")]
fn library_status(
    store: &crate::server::persistence::agent_library::AgentLibraryStore,
    req_id: u64,
    verified: &crate::server::auth::VerifiedRequestContext,
    mut request: eg_types::AgentLibraryStatusRequest,
) -> Response {
    let kind = request.kind;
    let purpose = match kind {
        eg_types::AgentLibraryMutationKind::Publish => "agent-library:publish",
        eg_types::AgentLibraryMutationKind::Retire => "agent-library:retire",
    };
    request.context = match bind_status_context(store, req_id, verified, request.context, purpose) {
        Ok(context) => context,
        Err(response) => return response,
    };
    match store.status(request) {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::storage::AgentLibraryStatus>(
                &result,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}
