//! Self-routing control-plane dispatch groups.

use super::*;

/// Cluster and shard administration: reshard/catalog/rebalance/backup/restore,
/// the placement catalog, Raft membership, cluster topology discovery and the
/// fleet server registry. All self-routing and cluster-wide, never graph-scoped.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_cluster_admin_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {

        // ── M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch) ──────
        // The wire surface that drives online resharding (EG-032), the tenant catalog
        // (EG-031) and the rebalance planner (EG-035) + its execution (EG-039). All
        // self-routing service-level ops handled here (not the per-graph chain), so they
        // reach the concrete redb backend via `as_redb`. A non-redb build returns a clean
        // "not available" error from the handler.
                method @ (Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        // ── Online backup / restore + PITR (CONCEPT:EG-KG.sharding.reshard-on-restore) ──────────
        // Routed through the SAME admin handler: self-routing service-level DR ops that
        // reach the concrete redb backend via `as_redb`. Non-redb builds return a clean
        // "not available" error from the handler.
        | Method::Backup { .. }
        | Method::Restore { .. }) => dispatch_cluster_admin_methods_arm_0(ctx, method).await,

        // ── Placement-catalog wire RPC (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4) ──
        // Self-routing, NOT graph-scoped (the catalog is cluster-wide, like the M3 admin
        // block above) — exposes the DIST-P2-1 `PlacementCatalog` over the wire so an
        // external caller (`epistemic_graph.client`'s `placement` namespace, AU's
        // `placement_catalog.py`) can consume it instead of guessing independently. A
        // A single-node engine returns an authoritative unplaced route. A configured
        // Raft node without MultiRaft is an invalid cluster and fails closed.
        // The admin mutation (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc, DIST-P2-5) is
        // routed through the SAME handler, self-routing exactly like the M3 admin
        // block above -- see `handlers::placement::try_handle`'s per-variant arms.
                method @ (Method::PlacementRoute { .. } | Method::PlacementAdmin { .. }) => dispatch_cluster_admin_methods_arm_1(ctx, method).await,

        // ── Raft cluster membership admin (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md §5
        // item 2) ── Self-routing, NOT graph-scoped (cluster-wide, like the M3 admin
        // block above): attaches/promotes a node against `MultiRaft` directly. Gated
        // `admin:cluster` by the SAME scope+admin enforcement every other admin-tier
        // method goes through above (`eg_capabilities::policy`), not a second check
        // here.
                method @ (Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. }) => dispatch_cluster_admin_methods_arm_2(ctx, method).await,

        // ── Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1) ──
        // Self-routing, NOT graph-scoped (cluster-wide, like the raft-admin block
        // above): `ClusterMembers` answers from ANY node's local `NodeInfoStore` +
        // live `MultiRaft` membership (no leader redirect, unlike `PlacementRoute` —
        // ADR-1's client resolves via any healthy seed). Node self-reports use a
        // typed internal Raft command and never enter public dispatch.
                method @ Method::ClusterMembers => dispatch_cluster_admin_methods_arm_3(ctx, method).await,

        // ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────
        // Self-routing, like `ClusterMembers` above, but for the
        // OPPOSITE reason: those are cluster-wide and NOT graph nodes, while this
        // writes a REAL `:Server` graph node into `__commons__` -- self-routes
        // here (rather than resolving `req.graph`) because a fleet server's
        // registration is a fleet-wide singleton concept, never tenant-scoped,
        // exactly like `ApplyMultisigMutation` self-routes before translating
        // into `Method::ApplyMutation` against `req.graph`. See
        // `handle_register_server`'s doc comment.
                method @ Method::RegisterServer {
            name,
            url,
            resources_json,
            ttl_secs,
        } => dispatch_cluster_admin_methods_arm_4(ctx, method).await,
        other => return ControlFlow::Continue(other),
    })
}

/// RF-020 Agent Library is self-routing and tenant-bound, but it is not part
/// of the cluster-admin backend. Keep its owner opening and typed operations
/// on the authenticated request context before the M3 admin handler resolves a
/// graph backend.

async fn dispatch_cluster_admin_methods_arm_0(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
                method @ (Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        // ── Online backup / restore + PITR (CONCEPT:EG-KG.sharding.reshard-on-restore) ──────────
        // Routed through the SAME admin handler: self-routing service-level DR ops that
        // reach the concrete redb backend via `as_redb`. Non-redb builds return a clean
        // "not available" error from the handler.
        | Method::Backup { .. }
        | Method::Restore { .. }) => {
            dispatch_boxed(
                async {
    let req_id = req.id;
    let req_agent_id = req.agent_id.clone();
    {
            match handlers::admin::try_handle(
                state,
                req_id,
                req_agent_id.as_deref(),
                verified_context.attempt_nonce(),
                method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is an admin method.
                Err(_) => Response::err(req_id, "admin dispatch routing error"),
            }
        }
}
            )
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

async fn dispatch_cluster_admin_methods_arm_1(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::PlacementRoute { .. } | Method::PlacementAdmin { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::placement::try_handle(state, req_id, method).await {
                        Ok(resp) => resp,
                        // Unreachable: every variant matched above is a placement method.
                        Err(_) => Response::err(req_id, "placement dispatch routing error"),
                    }
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

async fn dispatch_cluster_admin_methods_arm_2(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::raft_admin::try_handle(state, req_id, method).await {
                        Ok(resp) => resp,
                        // Unreachable: both variants matched above are raft-admin methods.
                        Err(_) => Response::err(req_id, "raft-admin dispatch routing error"),
                    }
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

async fn dispatch_cluster_admin_methods_arm_3(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ Method::ClusterMembers => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    match handlers::topology::try_handle(state, req_id, method, verified_context)
                        .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: the matched variant is the topology method.
                        Err(_) => Response::err(req_id, "cluster topology dispatch routing error"),
                    }
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

async fn dispatch_cluster_admin_methods_arm_4(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::RegisterServer {
            name,
            url,
            resources_json,
            ttl_secs,
        } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                {
                    handle_register_server(
                        state,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        name,
                        url,
                        resources_json,
                        ttl_secs,
                    )
                    .await
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}
pub(super) async fn dispatch_agent_library_methods(
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
        Method::AgentLibrary { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_library(state, req.id, verified_context, op).await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent Library is not available in this build (requires the `redb` feature)",
                )
            }
        }
        Method::AgentGraph { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_graph(state, req.id, verified_context, op).await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent graphs are not available in this build (requires the `redb` feature)",
                )
            }
        }
        Method::AgentComponent { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_component(state, req.id, verified_context, op)
                        .await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent components are not available in this build (requires `redb`)",
                )
            }
        }
        Method::SemanticIndex { op } => {
            #[cfg(all(feature = "ann-redb", feature = "query"))]
            {
                dispatch_boxed(async {
                    handlers::semantic_index::handle_semantic_index(
                        state,
                        req.id,
                        verified_context,
                        op,
                    )
                    .await
                })
                .await
            }
            #[cfg(not(all(feature = "ann-redb", feature = "query")))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "The semantic index is not available in this build (requires `ann-redb` and `query`)",
                )
            }
        }
        Method::AgentTemplate { op } => {
            #[cfg(feature = "redb")]
            {
                dispatch_boxed(async {
                    handlers::admin::handle_agent_template(state, req.id, verified_context, op)
                        .await
                })
                .await
            }
            #[cfg(not(feature = "redb"))]
            {
                let _ = (state, verified_context, op);
                Response::err(
                    req.id,
                    "Agent templates are not available in this build (requires `redb`)",
                )
            }
        }
        other => return ControlFlow::Continue(other),
    })
}

/// Self-routing compute and media surfaces: the durable analytics-job plane,
/// statechart programs, quantum programs, speech recognition and static
/// visualization export. Each runs a program or a model rather than touching
/// the graph directly, so none of them is identity or admin — the pre-domain
/// cut had all five in `dispatch_identity_and_admin_methods`.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_compute_and_media_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(any(
        feature = "jobs",
        feature = "statechart",
        feature = "quantum-agent-api",
        feature = "asr-whisper",
        feature = "viz-static-export"
    )))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(any(
        feature = "jobs",
        feature = "statechart",
        feature = "quantum-agent-api",
        feature = "asr-whisper",
        feature = "viz-static-export"
    ))]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Durable analytics-job plane (CONCEPT:INT-P2-1, feature `jobs`) ──────────
            // NOT graph-scoped (own `jobs.redb`, keyed by `job_id`) — self-routes here,
            // BEFORE the per-graph `dispatch_graph_op` chain, exactly like `TsAppend`/
            // `Kv*`/`CreateChannel` above. See `handlers/jobs.rs` module docs.
            #[cfg(feature = "jobs")]
            method @ Method::AnalyticsJob { op } => {
                dispatch_compute_and_media_methods_arm_0(ctx, method).await
            }

            // ── Native statechart engine (CONCEPT:INT-P2-2, feature `statechart`) ───────
            // NOT graph-scoped (own `statecharts.redb`, keyed by def_id/instance_id) —
            // self-routes here, BEFORE the per-graph `dispatch_graph_op` chain, exactly
            // like `AnalyticsJob` above. See `handlers/statechart.rs` module docs.
            #[cfg(feature = "statechart")]
            method @ Method::Statechart { op } => {
                dispatch_compute_and_media_methods_arm_1(ctx, method).await
            }

            // ── Agent-facing quantum control plane (Q8, CONCEPT:EG-KG.compute.quantum-agent-api,
            // feature `quantum-agent-api`) ──────────────────────────────────────────
            // NOT graph-scoped (pure compute -- reads no persisted graph state, writes
            // nothing durable) — self-routes here, BEFORE the per-graph `dispatch_graph_op`
            // chain, exactly like `AnalyticsJob`/`Statechart` above. See
            // `handlers::quantum`'s module docs for the full reachability/exactness/audit
            // contract this closes (program doc: "no job-plane, no wire protocol Method,
            // and no KG concept mapping" — the wire protocol Method half ends here).
            #[cfg(feature = "quantum-agent-api")]
            method @ Method::Quantum { op } => {
                dispatch_compute_and_media_methods_arm_2(ctx, method).await
            }
            // ── Native ASR provider surface (GOC-33, `OWNER-VOICE-ASR`, feature
            // `asr-whisper`) ─────────────────────────────────────────────────
            // NOT graph-scoped (a transcription reads no persisted graph state and
            // commits no durable asr.result.v1 here) — self-routes here, BEFORE the
            // per-graph `dispatch_graph_op` chain, exactly like `Quantum`/`Viz` above.
            // See `handlers::asr`'s module doc for the authority boundary.
            #[cfg(feature = "asr-whisper")]
            method @ Method::Asr { op } => {
                dispatch_compute_and_media_methods_arm_3(ctx, method).await
            }
            // ── Native visualization render surface (D-VZ-1 lanes V4/V6, feature
            // `viz-static-export`) ──────────────────────────────────────────────
            // NOT graph-scoped (a render builds a FRESH ephemeral per-request
            // ColumnStore, never reads a live GraphCore) — self-routes here, BEFORE
            // the per-graph `dispatch_graph_op` chain, exactly like
            // `AnalyticsJob`/`Statechart` above. See `handlers/viz.rs` module docs.
            // Gated on `viz-static-export` (not bare `viz`, which `eg-types` alone
            // already gates the wire `Method::Viz` variant on) — a deliberate,
            // documented deviation: the handler needs a real ColumnStore + export
            // backend to do anything, which only exist at that tier.
            #[cfg(feature = "viz-static-export")]
            method @ Method::Viz { op } => {
                dispatch_compute_and_media_methods_arm_4(ctx, method).await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

#[cfg(feature = "jobs")]
async fn dispatch_compute_and_media_methods_arm_0(
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
        Method::AnalyticsJob { op } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    // Worker fencing identity and durable actor attribution come from the
                    // authenticated context, never from the unsigned request envelope's
                    // display/agent field.
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    handlers::jobs::handle(
                        state,
                        req_id,
                        &carrier,
                        verified_context.attempt_nonce(),
                        verified_context.allows_analytics_worker(),
                        op,
                    )
                    .await
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "statechart")]
async fn dispatch_compute_and_media_methods_arm_1(
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
        Method::Statechart { op } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    // Durable owner attribution comes from the authenticated context, never
                    // from the unsigned request envelope's display/agent field.
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    handlers::statechart::handle(state, req_id, &carrier, op).await
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "quantum-agent-api")]
async fn dispatch_compute_and_media_methods_arm_2(
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
        Method::Quantum { op } => handlers::quantum::handle(req.id, op).await,
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "asr-whisper")]
async fn dispatch_compute_and_media_methods_arm_3(
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
        Method::Asr { op } => handlers::asr::handle(req.id, op).await,
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "viz-static-export")]
async fn dispatch_compute_and_media_methods_arm_4(
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
        Method::Viz { op } => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    // No durable owner-scoped state to attribute a render to; the carrier
                    // is still resolved for parity with the sibling self-routed handlers
                    // (see `handlers::viz::handle`'s doc).
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    handlers::viz::handle(state, req_id, &carrier, op).await
                }
            })
            .await
        }
        other => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}
