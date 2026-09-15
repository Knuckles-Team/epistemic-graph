#[cfg(feature = "raft")]
use super::change_envelope::multi_graph_batch_update;
#[cfg(feature = "raft")]
use super::consensus::{is_replicated_apply, propose_native_mutation};
use super::router::dispatch_request_method;
#[cfg(all(feature = "sparql-http", feature = "raft"))]
use super::sparql_update::coordinated_sparql_http_update;
use super::*;

mod authorization;
#[cfg(feature = "raft")]
mod consensus;
mod preflight;
mod saga;
mod screen;

pub(super) use authorization::{
    check_scope_and_admin_authority, check_submit_work_item_context, DispatchAuthority,
};

impl DispatchAuthority {
    pub(super) const fn state_machine_authorized(self) -> bool {
        self.state_machine_authorized
    }

    pub(super) const fn identity_bootstrap(self) -> bool {
        self.identity_bootstrap
    }
}
#[cfg(feature = "raft")]
pub(super) use consensus::{
    check_cluster_placement_before_consensus, route_consensus_before_gateway,
};
pub(super) use preflight::preflight_request_msgpack;
#[cfg(all(test, feature = "ast"))]
pub(super) use preflight::AstInputLimits;
#[cfg(feature = "ast")]
pub(super) use preflight::{ast_input_limits, decode_ast_files, validate_ast_logical_path};
#[cfg(feature = "redb")]
use saga::replayed_response;
pub(super) use saga::{begin_session_control_saga, finalize_dispatch_response};
pub(super) use screen::decode_screen_observation;

/// Dispatch a single request to the appropriate handler, recording
/// per-operation request counters and latency (CONCEPT:EG-KG.txn.per-graph-write-isolation).
pub async fn dispatch(state: &Arc<RwLock<ServerState>>, req: Request) -> Response {
    dispatch_with_context(state, req, None).await
}

pub(super) fn append_native_resource_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend([
            "ReserveWorkItemResources",
            "ReleaseWorkItemResources",
            "ReclaimWorkItemResources",
            "QueryWorkItemReservation",
            "ResourceReservationStatus",
            "UpdateResourceHost",
        ]);
    }
}

pub(super) fn append_native_capacity_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend([
            "AcquireCapacity",
            "RenewCapacity",
            "ReleaseCapacity",
            "ReclaimExpiredCapacity",
            "ReconcileCapacity",
            "CapacityStatus",
            "UpdateCapacityCell",
        ]);
    }
}

pub(super) fn append_native_work_item_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend(["KgDelegate", "SubmitWorkItem", "SubmitWorkItems"]);
    }
}

/// Dispatch a native transport request whose current envelope was verified
/// before optional QoS admission. This keeps authentication single-pass: a
/// mutation nonce is consumed by the durable MutationBatch ledger and a
/// read-only nonce by the transport replay ledger, while admission plus
/// dispatch share the same immutable verified context.
pub(crate) async fn dispatch_verified_request(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    context: VerifiedRequestContext,
) -> Response {
    dispatch_with_context(state, req, Some(context)).await
}

/// Bridge an already-authenticated auxiliary broker protocol into the same
/// authorization and dispatch path as the primary request transport. The
/// caller supplies only a secret-keyed opaque actor reference; raw protocol
/// usernames are never admitted to request or persistence state.
pub(crate) async fn dispatch_authenticated_broker_actor(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    actor_ref: &str,
) -> Response {
    let request_id = req.id;
    let context = match VerifiedRequestContext::authenticated_broker_actor(actor_ref, request_id) {
        Ok(context) => context,
        Err(error) => {
            crate::metrics::auth_failure();
            return Response::err(request_id, error);
        }
    };
    dispatch_with_context(state, req, Some(context)).await
}

/// Dispatch an engine-owned local query adapter under its fixed, read-only
/// service identity. The caller remains subject to provisioned graph ACL/RBAC.
pub(crate) async fn dispatch_authenticated_local_query(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
) -> Response {
    let context = match VerifiedRequestContext::authenticated_local_query(req.id) {
        Ok(context) => context,
        Err(error) => return Response::err(req.id, error),
    };
    dispatch_with_context(state, req, Some(context)).await
}

pub(super) async fn dispatch_with_context(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    context: Option<VerifiedRequestContext>,
) -> Response {
    // CONCEPT:EG-OS.observability.slow-query-descriptor — slow-query descriptor, captured BEFORE the method is moved
    // into `dispatch_inner`. `None` (zero cost) unless EPISTEMIC_GRAPH_SLOW_QUERY_MS
    // enabled it AND this is a query method.
    let slow = crate::slow_query::describe(&req.method);
    #[cfg(feature = "metrics")]
    let op: &'static str = (&req.method).into();

    // Time the request when EITHER Prometheus metrics OR slow-query logging needs
    // it. When both are off (metrics feature disabled AND the threshold unset) we
    // skip the clock entirely — byte-for-byte the prior `not(metrics)` path.
    let start = (cfg!(feature = "metrics") || slow.is_some()).then(std::time::Instant::now);

    let resp = dispatch_inner(state, req, context).await;

    if let Some(start) = start {
        let elapsed = start.elapsed();
        #[cfg(feature = "metrics")]
        crate::metrics::record_request(op, elapsed.as_secs_f64());
        if let Some(slow) = slow {
            slow.log_if_slow(elapsed);
        }
    }
    resp
}

#[cfg(feature = "cost")]
pub(super) async fn dispatch_resource_stats(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    request: crate::cost::ResourceStatsRequest,
) -> Response {
    // ResourceStats is service-scoped, so construct the same verified graph
    // read authority used by graph reads before scanning the registry.  The
    // cost collector filters tenant + ACL before it increments any aggregate,
    // cursor, or candidate state.
    let isolation = timed_read(state).await.isolation.clone();
    let authority = match GraphReadAuthority::from_verified(verified_context, &isolation) {
        Ok(authority) => authority,
        Err(error) => return Response::err(req_id, error),
    };
    match crate::cost::collect_resource_stats_authorized(
        state,
        &authority,
        verified_context.tenant(),
        request,
    )
    .await
    {
        Ok(snapshot) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::coordination::ResourceStatsPage>(
                snapshot,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// Explicitly erases a dispatch-arm future's concrete (often enormous,
/// datafusion/cypher/sql-plan-carrying) type to `dyn Future + Send` at the
/// match-arm boundary, once, here -- rather than letting `dispatch_inner`'s
/// own generated per-arm state machine hold N structurally distinct
/// concrete future types simultaneously, which is what overflowed rustc's
/// trait-resolution recursion limit (E0275) once this lane's extraction
/// gave the match 53 separate `async fn` calls instead of one inlined body.
/// `Box<dyn Future<Output = Response> + Send>` is trivially `Send` by
/// construction, so this bounds the Send-proof cost per arm to O(1) instead
/// of O(the whole call graph). Same fix as `server::transport.rs`'s spawn
/// site and the pre-existing `dispatch_on_heap` test helper (dispatch.rs /
/// registry_reaper.rs), applied at the dispatch_inner match itself.
pub(super) fn dispatch_boxed<'a>(
    fut: impl std::future::Future<Output = Response> + Send + 'a,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
    Box::pin(fut)
}

async fn compute_identity_bootstrap(
    state: &Arc<RwLock<ServerState>>,
    req: &Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
) -> bool {
    !state_machine_authorized && {
        let state = timed_read(state).await;
        state.isolation.identity_bootstrap_pending()
            && req.graph == "__commons__"
            && matches!(
                &req.method,
                Method::RegisterIdentity {
                    agent_id,
                    role: crate::isolation::AgentRole::System,
                    teams,
                    roles,
                    ..
                } if agent_id == verified_context.agent_id()
                    && teams.is_empty()
                    && roles.is_empty()
            )
            && verified_context.allows_identity_bootstrap()
    }
}

async fn preflight_commit_owner(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req: &Request,
    verified_context: &VerifiedRequestContext,
) -> Result<(), Response> {
    let Method::Commit { txn_id, .. } = &req.method else {
        return Ok(());
    };
    crate::server::handlers::txn::preflight_open_txn_authority(
        state,
        req_id,
        verified_context,
        txn_id,
    )
    .await
}

async fn dispatch_preamble_checks(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
) -> Result<(Request, DispatchAuthority), Response> {
    let method_policy = eg_capabilities::policy(&req.method);
    let action = method_policy.authz_action;
    let identity_bootstrap =
        compute_identity_bootstrap(state, &req, verified_context, state_machine_authorized).await;
    let authority = DispatchAuthority {
        state_machine_authorized,
        identity_bootstrap,
    };
    // Resource reservation authority is deliberately narrower than the coarse
    // `kg:write` aggregate.  The resolved-profile assertion and host telemetry
    // are controller inputs; an ordinary graph writer must not be able to forge
    // a heavy reservation or overwrite shared physical-host accounting merely by
    // carrying a WorkItem-shaped request.  Replicated apply already carries a
    // verified native authority and bypasses the external gate here.
    check_scope_and_admin_authority(
        state,
        &req,
        verified_context,
        authority,
        action,
        method_policy.mutates,
    )
    .await?;

    if let Err(error) = preflight_request_msgpack(&req.method) {
        return Err(Response::err(req.id, error));
    }

    // BUG-254 / NE-023: reject an unsupported lifecycle type at the first
    // authenticated, authoritative dispatch boundary.  This runs before
    // placement routing, consensus proposal, session-control saga creation,
    // persistence, or registry publication, so a bad CreateGraph request can
    // never enter the multi-minute retry path or leave partial state behind.
    // The transport decoder has a matching closed-enum guard for raw wire
    // callers; this check protects in-process and future-variant callers too.
    if let Method::CreateGraph { graph_type, .. } = &req.method {
        if let Err(error) = validate_graph_create_type(*graph_type) {
            return Err(Response::err(req.id, error));
        }
    }

    // A transaction id is a routing handle, never a bearer credential. Check an
    // open Commit owner before local or consensus routing; durable keyed replay
    // remains in the post-removal receipt boundary.
    preflight_commit_owner(state, req.id, &req, verified_context).await?;

    check_submit_work_item_context(&req, verified_context, authority)?;

    // Cluster writes cross consensus at the authenticated request boundary. The
    // complete mutation inventory is partitioned between graph commands and typed
    // native commands; a command constructor failure is returned before any local
    // saga or store mutation.
    #[cfg(feature = "raft")]
    check_cluster_placement_before_consensus(state, &req).await?;

    #[cfg(feature = "raft")]
    let req = route_consensus_before_gateway(
        state,
        req,
        verified_context,
        authority.identity_bootstrap(),
    )
    .await?;

    Ok((req, authority))
}

async fn dispatch_inner(
    state: &Arc<RwLock<ServerState>>,
    mut req: Request,
    context: Option<VerifiedRequestContext>,
) -> Response {
    // External requests verify the current signed context. Mutation nonces are
    // carried into the durable MutationBatch kernel, while read-only nonces are
    // checked by the transport replay ledger before dispatch. In-process broker
    // bridges provide a context only after their protocol-specific credential has
    // verified.
    let verified_context = match context {
        Some(context) => context,
        None => {
            let s = timed_read(state).await;
            match verify_request_with_security_dir(&s.auth_secret, &req, s.persist_dir.as_deref()) {
                Ok(context) => context,
                Err(msg) => {
                    crate::metrics::auth_failure();
                    return Response::err(req.id, msg);
                }
            }
        }
    };
    req.agent_id = Some(verified_context.agent_id().to_string());

    #[cfg(feature = "raft")]
    let state_machine_authorized = is_replicated_apply();
    #[cfg(not(feature = "raft"))]
    let state_machine_authorized = false;

    // ── Scope + admin enforcement (CONCEPT:EG-KG.compute.feature, EG-P0-6) ────────────────
    // A verified context must carry the capability ledger's action scope.
    // Additionally gates EVERY method whose policy declares a system-wide
    // admin `authz_action` (RegisterIdentity/RbacAdmin/ApplyMultisigMutation, the
    // M3 reshard/rebalance/catalog family, Backup/Restore) behind
    // `IsolationLayer::has_admin_capability` -- driven off the ledger's
    // `authz_action` string (`access::is_admin_authz_action`), NEVER a second
    // hardcoded method-name list here. Checked ONCE, before the method match, so
    // every current AND future admin-tier method is covered without a dispatch.rs
    // edit. Runs only when the `server` feature (which pulls in `eg-capabilities`)
    // is active -- always true for this binary.
    let (req, authority) =
        match dispatch_preamble_checks(state, req, &verified_context, state_machine_authorized)
            .await
        {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    dispatch_admitted_request(state, req, &verified_context, authority).await
}

async fn dispatch_admitted_request(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    authority: DispatchAuthority,
) -> Response {
    let session_control = match begin_session_control_saga(
        state,
        req.id,
        verified_context,
        &req.method,
        verified_context.attempt_nonce(),
    )
    .await
    {
        Ok(control) => control,
        Err(error) => return Response::err(req.id, error),
    };
    if let Some(control) = session_control.as_ref() {
        #[cfg(feature = "redb")]
        if let Some(response) = replayed_response(req.id, control, &req.method) {
            return response;
        }
        #[cfg(not(feature = "redb"))]
        let _ = control;
    }

    #[cfg(feature = "redb")]
    let saga_authority = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(error) => return Response::err(req.id, error),
    };
    let operation = async {
        let req_id = req.id;
        let method_for_finalize = req.method.clone();
        let response = dispatch_request_method(state, req, verified_context, authority).await;
        finalize_dispatch_response(req_id, response, &method_for_finalize, session_control)
    };
    #[cfg(feature = "redb")]
    return handlers::admin::scope_admin_saga_authority(saga_authority, operation).await;
    #[cfg(not(feature = "redb"))]
    operation.await
}

#[cfg(test)]
mod native_resource_capability_tests {
    use super::append_native_resource_ops;

    #[test]
    fn native_resource_ops_are_advertised_only_when_backend_declares_support() {
        let mut dark = Vec::new();
        append_native_resource_ops(&mut dark, false);
        assert!(dark.is_empty());

        let mut served = Vec::new();
        append_native_resource_ops(&mut served, true);
        assert_eq!(
            served,
            vec![
                "ReserveWorkItemResources",
                "ReleaseWorkItemResources",
                "ReclaimWorkItemResources",
                "QueryWorkItemReservation",
                "ResourceReservationStatus",
                "UpdateResourceHost",
            ]
        );
    }
}
