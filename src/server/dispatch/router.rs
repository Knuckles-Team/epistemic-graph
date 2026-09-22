use super::change_envelope::{dispatch_change_envelopes, multi_graph_batch_update};
use super::consensus::{
    handle_list_registered_servers, handle_register_server,
    replicated_identity_bootstrap_authorized,
};
use super::graph_pipeline::dispatch_graph_op;
#[cfg(feature = "knowledge-batch")]
use super::graph_pipeline::dispatch_knowledge_stream;
#[cfg(feature = "modality-serving")]
use super::graph_pipeline::dispatch_served_modality;
#[cfg(feature = "cost")]
use super::request_boundary::dispatch_resource_stats;
use super::request_boundary::{
    append_native_capacity_ops, append_native_resource_ops, append_native_work_item_ops,
    decode_screen_observation, dispatch_boxed, DispatchAuthority,
};
#[cfg(feature = "ast")]
use super::request_boundary::{ast_input_limits, decode_ast_files, validate_ast_logical_path};
#[cfg(feature = "sparql-http")]
use super::sparql_update::coordinated_sparql_http_update;
use super::*;

mod channels;
mod control_plane;
mod data_plane;
mod data_plane_arms;
mod decision_plane;
mod graph_lifecycle;
mod identity_access;
mod lifecycle;
mod resource_cost;
mod service_control;
mod source_ingest;

use channels::dispatch_channel_methods;
use control_plane::{
    dispatch_agent_library_methods, dispatch_cluster_admin_methods,
    dispatch_compute_and_media_methods,
};
#[cfg(feature = "query")]
use data_plane::dispatch_sql_source_methods;
use decision_plane::dispatch_decision_plane_methods;

use data_plane::{
    dispatch_change_envelope_methods, dispatch_governed_stream_write_methods,
    dispatch_method_scoped_graph_methods, dispatch_store_methods, dispatch_streaming_methods,
    dispatch_transaction_methods,
};
use graph_lifecycle::dispatch_graph_lifecycle_methods;
use identity_access::dispatch_identity_and_access_methods;
#[cfg(feature = "security")]
use lifecycle::apply_rbac_admin;
use lifecycle::{
    create_graph, delete_graph, dispatch_get_identity, index_kind_label, index_validity_label,
};
use resource_cost::dispatch_resource_cost_methods;
use service_control::dispatch_service_control_methods;
use source_ingest::dispatch_source_ingest_methods;

/// The request fields every dispatch group needs after `req.method` has been
/// moved out of the `Request`. Keeping the field names (`id`, `graph`,
/// `agent_id`) means each relocated match arm still reads exactly as it did
/// inside `dispatch_inner`.
struct DispatchHeader {
    id: u64,
    graph: String,
    agent_id: Option<String>,
}

/// The authenticated, preamble-resolved context shared by every dispatch group.
/// All fields are shared references or flags, so this is `Copy` and each group
/// call is free.
#[derive(Clone, Copy)]
struct DispatchCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req: &'a DispatchHeader,
    verified_context: &'a VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
}

/// The one guarded arm: a SPARQL-over-HTTP UPDATE arrives as an
/// `ApplyMutation` whose event type is the SPARQL-HTTP update marker. Any
/// other `ApplyMutation` falls through to the ordinary per-graph chain,
/// exactly as the guard's own fallthrough did.
#[cfg(feature = "sparql-http")]
async fn dispatch_sparql_http_update(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        Method::ApplyMutation { event_type, query }
            if event_type == crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT =>
        {
            coordinated_sparql_http_update(
                state,
                req.id,
                req.agent_id.as_deref(),
                verified_context,
                &req.graph,
                query,
            )
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// The control plane: service control, source ingestion, cost telemetry, graph
/// lifecycle, cluster administration, channels, identity/access and the
/// compute/media surfaces — everything resolved before the per-graph data path.
///
/// Each link is a `?` on `ControlFlow`: `Break(response)` short-circuits (the
/// group handled it), `Continue(method)` hands the method to the next group.
async fn dispatch_control_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let method = dispatch_service_control_methods(ctx, method).await?;
    let method = dispatch_source_ingest_methods(ctx, method).await?;
    let method = dispatch_resource_cost_methods(ctx, method).await?;
    let method = dispatch_graph_lifecycle_methods(ctx, method).await?;
    let method = dispatch_agent_library_methods(ctx, method).await?;
    let method = dispatch_decision_plane_methods(ctx, method).await?;
    let method = dispatch_cluster_admin_methods(ctx, method).await?;
    let method = dispatch_channel_methods(ctx, method).await?;
    let method = dispatch_identity_and_access_methods(ctx, method).await?;
    dispatch_compute_and_media_methods(ctx, method).await
}

/// The data plane: transactions, the non-graph stores, the subscription plane,
/// the governed stream writes, change-envelope replication, the method-scoped
/// graph targets and the guarded SPARQL-over-HTTP update. A method none of these
/// claims is handed back for the ordinary per-graph chain.
async fn dispatch_data_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let method = dispatch_transaction_methods(ctx, method).await?;
    let method = dispatch_store_methods(ctx, method).await?;
    #[cfg(feature = "query")]
    let method = dispatch_sql_source_methods(ctx, method).await?;
    let method = dispatch_streaming_methods(ctx, method).await?;
    let method = dispatch_governed_stream_write_methods(ctx, method).await?;
    let method = dispatch_change_envelope_methods(ctx, method).await?;
    let method = dispatch_method_scoped_graph_methods(ctx, method).await?;
    #[cfg(feature = "sparql-http")]
    {
        dispatch_sparql_http_update(ctx, method).await
    }
    #[cfg(not(feature = "sparql-http"))]
    {
        ControlFlow::Continue(method)
    }
}

/// Route one authenticated request to its handler.
///
/// The single 59-arm `match req.method` this replaced is now fifteen group
/// dispatchers, one per DOMAIN, over disjoint `Method` variants, tried in
/// order; each hands a method it does not own back as
/// `ControlFlow::Continue`. A method no group claims falls through to the
/// ordinary per-graph chain, which is exactly what the old `_` arm did.
pub(super) async fn dispatch_request_method(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    authority: DispatchAuthority,
) -> Response {
    let method = req.method;
    let req = DispatchHeader {
        id: req.id,
        graph: req.graph,
        agent_id: req.agent_id,
    };
    let ctx = DispatchCtx {
        state,
        req: &req,
        verified_context,
        state_machine_authorized: authority.state_machine_authorized(),
        identity_bootstrap: authority.identity_bootstrap(),
    };
    let method = match dispatch_control_plane_methods(ctx, method).await {
        ControlFlow::Break(response) => return response,
        ControlFlow::Continue(method) => method,
    };
    let method = match dispatch_data_plane_methods(ctx, method).await {
        ControlFlow::Break(response) => return response,
        ControlFlow::Continue(method) => method,
    };
    dispatch_graph_op(
        state,
        &req.graph,
        req.id,
        req.agent_id.as_deref(),
        verified_context,
        method,
    )
    .await
}
