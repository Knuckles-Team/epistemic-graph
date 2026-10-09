use super::*;

/// Identity and access control: principal registration and read-back, policy
/// export, RBAC administration and the multisig-governed mutation admission
/// path. `RegisterIdentity` belongs HERE — the pre-domain cut left it in the
/// channel group.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_identity_and_access_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Zero-Trust Consensus ─────────────────────────────────────────
        method @ Method::RegisterIdentity { .. } => {
            dispatch_identity_and_access_methods_arm_0(ctx, method).await
        }
        // Identity read-back (CONCEPT:EG-KG.compute.feature): closes the `RegisterIdentity`
        // blind-upsert gap. `RegisterIdentity` REPLACES a principal's whole role set on
        // every call, so a caller that wants to add a role without dropping one already
        // granted by a prior admission pass must read the current set back first. Gated at
        // the SAME `security:admin` scope as `RegisterIdentity` (see `eg_capabilities::policy`),
        // enforced by the admin-scope check above the method match, so no additional
        // authorization is done here.
        method @ Method::GetIdentity { .. } => {
            dispatch_identity_and_access_methods_arm_1(ctx, method).await
        }
        // A principal's CURRENT access to the request graph, for an executor
        // acting on that principal's behalf (gated `security:check`).
        Method::CheckAccess { agent_id, access } => {
            dispatch_boxed(dispatch_check_access(ctx, agent_id, access)).await
        }

        // ── RBAC policy administration (CONCEPT:EG-KG.compute.feature) ──────────────────
        // Gated at the handler; a non-security build has no arm and falls to the
        // dispatch "not available in this build" catch-all (mirrors EG-090).
        #[cfg(feature = "security")]
        method @ Method::RbacAdmin { .. } => {
            dispatch_identity_and_access_methods_arm_2(ctx, method).await
        }

        method @ Method::ApplyMultisigMutation { .. } => {
            dispatch_identity_and_access_methods_arm_3(ctx, method).await
        }

        // ── Just-in-time RBAC elevation (EH-404) ──────────────────────────
        // Security-only like `RbacAdmin`: the ledger lives in the RBAC image.
        #[cfg(feature = "security")]
        Method::RbacElevation { op, actor } => {
            crate::server::dispatch::elevation::dispatch_rbac_elevation(
                state,
                req.id,
                verified_context,
                crate::server::dispatch::elevation::ElevationStampAuthority::of(
                    state_machine_authorized,
                ),
                (op, actor),
            )
            .await
        }
        // ── Governed changes (EH-560) ─────────────────────────────────────
        #[cfg(feature = "security")]
        Method::GovernedChange { op, actor } => {
            crate::server::dispatch::governed::dispatch_governed_change(
                state,
                req.id,
                verified_context,
                crate::server::dispatch::elevation::ElevationStampAuthority::of(
                    state_machine_authorized,
                ),
                (op, actor),
            )
            .await
        }
        other => return dispatch_identity_store_method(ctx, other).await,
    })
}

/// Keep the identity-store route in this domain, including feature fallback.
async fn dispatch_identity_store_method(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[cfg(not(feature = "security"))]
    let _ = ctx;
    match method {
        #[cfg(feature = "security")]
        Method::Identity { op, stamp } => ControlFlow::Break(
            crate::server::dispatch::identity_store::dispatch_identity(
                ctx.state,
                ctx.req.id,
                ctx.verified_context,
                crate::server::dispatch::elevation::ElevationStampAuthority::of(
                    ctx.state_machine_authorized,
                ),
                (op, stamp),
            )
            .await,
        ),
        other => ControlFlow::Continue(other),
    }
}

/// `ApplyMultisigMutation` answers with the SPARQL UPDATE report of the
/// `ApplyMutation` it was translated into, declared under its own marker.
async fn dispatch_identity_and_access_methods_arm_0(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::RegisterIdentity {
            agent_id,
            role,
            teams,
            signature,
            roles,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_graph = req.graph.clone();
                {
                    let verification = if state_machine_authorized {
                        Ok(())
                    } else {
                        verify_register_identity_signature(
                            verified_context,
                            &req_graph,
                            &agent_id,
                            &role,
                            &teams,
                            &roles,
                            &signature,
                        )
                    };
                    if let Err(message) = verification {
                        crate::metrics::auth_failure();
                        return Response::err(req_id, message);
                    }
                    let mut s = timed_write(state).await;
                    let identity = crate::isolation::AgentIdentity {
                        agent_id: agent_id.clone(),
                        role,
                        teams,
                        roles,
                    };
                    if let Err(message) = register_identity(
                        &mut s,
                        identity,
                        IdentityRegistrationMode {
                            bootstrap: identity_bootstrap,
                            state_machine_authorized,
                            actor: verified_context.principal(),
                        },
                    ) {
                        return Response::err(req_id, message);
                    }
                    info!("RegisterIdentity committed");
                    Response::ok(
                        req_id,
                        ResultPayload::scalar::<
                            eg_types::result_contract::security::RegisterIdentity,
                        >("registered".to_string()),
                    )
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

/// `CheckAccess`: the caller must itself read the request graph; the answer
/// is the engine's own admission decision for `agent_id` on that graph.
async fn dispatch_check_access(
    ctx: DispatchCtx<'_>,
    agent_id: String,
    access: eg_types::acl::AccessCheck,
) -> Response {
    let (req_id, graph) = (ctx.req.id, ctx.req.graph.clone());
    let s = timed_read(ctx.state).await;
    let Some(entry) = s.registry.get(&graph) else {
        return Response::err(req_id, format!("Graph '{graph}' not found"));
    };
    let caller = Some(ctx.verified_context.agent_id());
    let owner = entry.owner.as_deref();
    if let Err(denied) = crate::server::access::check_graph_access(
        &s.isolation,
        caller,
        &graph,
        entry.graph_type,
        owner,
        crate::isolation::AccessLevel::Read,
    ) {
        return Response::err(req_id, denied);
    }
    let allowed = crate::server::access::principal_may_access(
        &s.isolation,
        &agent_id,
        &graph,
        entry.graph_type,
        owner,
        access,
    );
    drop(s);
    let decision = eg_types::acl::AccessDecision {
        agent_id,
        graph,
        access,
        allowed,
    };
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::security::CheckAccess>(decision),
    )
}

struct IdentityRegistrationMode<'a> {
    bootstrap: bool,
    state_machine_authorized: bool,
    /// Who registered, for the identity store's own audit entry.
    actor: &'a str,
}

fn register_identity(
    state: &mut ServerState,
    identity: crate::isolation::AgentIdentity,
    mode: IdentityRegistrationMode<'_>,
) -> Result<(), String> {
    if mode.bootstrap
        || (mode.state_machine_authorized && replicated_identity_bootstrap_authorized())
    {
        return state.isolation.try_bootstrap_system_identity(identity);
    }
    #[cfg(feature = "security")]
    {
        let actor = crate::isolation::AuditActor {
            principal: mode.actor,
            now_ms: authoritative_now_ms(),
        };
        let outcome = state.isolation.try_register_agent_audited(identity, actor);
        state.publish_identity_view();
        outcome
    }
    #[cfg(not(feature = "security"))]
    {
        let _ = mode.actor;
        state.isolation.try_register_agent_from_request(identity)
    }
}

async fn dispatch_identity_and_access_methods_arm_1(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::GetIdentity { agent_id } => {
            dispatch_boxed(dispatch_get_identity(state, req.id, &req.graph, agent_id)).await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "security")]
async fn dispatch_identity_and_access_methods_arm_2(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::RbacAdmin { op } => {
            let actor = verified_context.principal();
            dispatch_boxed(apply_rbac_admin(state, req.id, op, actor)).await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

async fn dispatch_identity_and_access_methods_arm_3(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    if !state_machine_authorized {
                        if let Err(message) = verify_multisig_mutation_signatures(
                            verified_context,
                            &req_graph,
                            &signatures,
                            threshold,
                            &mutation_type,
                            &query,
                        ) {
                            crate::metrics::auth_failure();
                            return Response::err(req_id, message);
                        }
                    }
                    // Delegate mutation application to the target graph
                    multisig_mutation_response(
                        dispatch_graph_op(
                            state,
                            &req_graph,
                            req_id,
                            req_agent_id.as_deref(),
                            verified_context,
                            Method::ApplyMutation {
                                event_type: mutation_type,
                                query,
                            },
                        )
                        .await,
                    )
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}
fn multisig_mutation_response(response: Response) -> Response {
    if let Some(refusal) = response.refusal_text() {
        return Response::err(response.id, refusal);
    }
    let Response { id, result, .. } = response;
    let report = match result {
        Some(ResultPayload::Json(value)) => serde_json::from_value::<
            eg_types::result_contract::transactions::SparqlUpdateReport,
        >(value)
        .map_err(|error| {
            format!(
                "ApplyMultisigMutation: the translated ApplyMutation report is invalid: {error}"
            )
        }),
        _ => Err(
            "ApplyMultisigMutation: the translated ApplyMutation answered no report".to_string(),
        ),
    };
    Response::ok(
        id,
        report.and_then(
            ResultPayload::of::<eg_types::result_contract::transactions::ApplyMultisigMutation>,
        ),
    )
}
