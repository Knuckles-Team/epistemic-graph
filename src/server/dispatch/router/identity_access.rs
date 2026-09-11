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
                    if !state_machine_authorized {
                        if let Err(message) = verify_register_identity_signature(
                            verified_context,
                            &req_graph,
                            &agent_id,
                            &role,
                            &teams,
                            &roles,
                            &signature,
                        ) {
                            crate::metrics::auth_failure();
                            return Response::err(req_id, message);
                        }
                    }
                    let mut s = timed_write(state).await;
                    let identity = crate::isolation::AgentIdentity {
                        agent_id: agent_id.clone(),
                        role,
                        teams,
                        roles,
                    };
                    let result = if identity_bootstrap
                        || (state_machine_authorized && replicated_identity_bootstrap_authorized())
                    {
                        s.isolation.try_bootstrap_system_identity(identity)
                    } else {
                        s.isolation.try_register_agent_from_request(identity)
                    };
                    if let Err(message) = result {
                        return Response::err(req_id, message);
                    }
                    info!("RegisterIdentity committed");
                    Response::ok(req_id, ResultPayload::String("registered".to_string()))
                }
            })
            .await
        }
        // Identity read-back (CONCEPT:EG-KG.compute.feature): closes the `RegisterIdentity`
        // blind-upsert gap. `RegisterIdentity` REPLACES a principal's whole role set on
        // every call, so a caller that wants to add a role without dropping one already
        // granted by a prior admission pass must read the current set back first. Gated at
        // the SAME `security:admin` scope as `RegisterIdentity` (see `eg_capabilities::policy`),
        // enforced by the admin-scope check above the method match, so no additional
        // authorization is done here.
        Method::GetIdentity { agent_id } => {
            dispatch_boxed(dispatch_get_identity(state, req.id, &req.graph, agent_id)).await
        }

        // ── RBAC policy administration (CONCEPT:EG-KG.compute.feature) ──────────────────
        // Gated at the handler; a non-security build has no arm and falls to the
        // dispatch "not available in this build" catch-all (mirrors EG-090).
        #[cfg(feature = "security")]
        Method::RbacAdmin { op } => dispatch_boxed(apply_rbac_admin(state, req.id, op)).await,

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
                    .await
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}
