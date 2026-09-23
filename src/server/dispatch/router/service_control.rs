use super::*;

/// Process-level service control: liveness, readiness, in-flight request
/// cancellation and shutdown. None of these resolves a graph.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_service_control_methods(
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
        // ── Service-level ────────────────────────────────────────────
        Method::Ping => Response::ok(
            req.id,
            ResultPayload::scalar::<eg_types::result_contract::cluster::Ping>("pong".to_string()),
        ),

        Method::Health => dispatch_boxed(health_response(state, req.id)).await,

        // L36: cooperative cancellation of an in-flight request by its `req_id` (CONCEPT:EG-KG.query.streaming-spillable-collect).
        // Service-level (no graph resolution needed — the registry is keyed by req_id,
        // process-wide) so it works regardless of which graph the target request is
        // running against. `false` covers every "nothing to cancel" case uniformly
        // (already finished / never cancellable / unknown id) — never an error.
        #[cfg(feature = "query")]
        Method::CancelRequest { target_req_id } => Response::ok(
            req.id,
            ResultPayload::scalar::<eg_types::result_contract::cluster::CancelRequest>(
                crate::server::request_cancel::cancel(target_req_id),
            ),
        ),

        Method::Shutdown => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    info!("Shutdown requested via protocol");
                    Response::ok(
                        req_id,
                        ResultPayload::scalar::<eg_types::result_contract::cluster::Shutdown>(
                            "shutting_down".to_string(),
                        ),
                    )
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// `Health`: liveness, graph materialization counts, and the served operation set.
async fn health_response(state: &Arc<RwLock<ServerState>>, req_id: u64) -> Response {
    let (
        lifecycle,
        native_resource_ops_available,
        native_capacity_ops_available,
        native_work_item_ops_available,
    ) = {
        let state = timed_read(state).await;
        let manifests = state.registry.materialization_manifests();
        let complete = manifests.iter().filter(|manifest| manifest.valid).count();
        let partial = manifests
            .iter()
            .filter(|manifest| manifest.phase == crate::registry::MaterializationPhase::Partial)
            .count();
        let failed = manifests
            .iter()
            .filter(|manifest| manifest.phase == crate::registry::MaterializationPhase::Failed)
            .count();
        (
            eg_types::result_contract::cluster::GraphLifecycleHealth {
                catalog_graphs: state.registry.catalog_len() as u64,
                resident_graphs: state.registry.resident_len() as u64,
                complete_graphs: complete as u64,
                partial_graphs: partial as u64,
                failed_graphs: failed as u64,
                all_resident_materializations_valid: partial == 0 && failed == 0,
            },
            state
                .persistence
                .as_ref()
                .is_some_and(|backend| backend.supports_native_resource_reservations()),
            state
                .persistence
                .as_ref()
                .is_some_and(|backend| backend.supports_native_capacity_leases()),
            state
                .persistence
                .as_ref()
                .is_some_and(|backend| backend.supports_native_work_item_submission()),
        )
    };
    let uptime_s = 0; // you can capture start time in ServerState
    let mem_bytes = 0;
    let mut served_ops = registry_served_ops();
    append_native_resource_ops(&mut served_ops, native_resource_ops_available);
    append_native_capacity_ops(&mut served_ops, native_capacity_ops_available);
    append_native_work_item_ops(&mut served_ops, native_work_item_ops_available);
    // ``version`` + ``ops`` let clients negotiate capabilities (e.g. only
    // use ``ParseFiles`` against an engine that advertises it) and fall
    // back gracefully against an older binary. (CONCEPT:EG-KG.query.dispatch-routing)
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::cluster::Health>(
            eg_types::result_contract::cluster::HealthReport {
                status: "ok".to_string(),
                uptime_s,
                mem_bytes,
                version: env!("CARGO_PKG_VERSION").to_string(),
                graph_lifecycle: lifecycle,
                ops: served_ops.into_iter().map(str::to_string).collect(),
            },
        ),
    )
}

/// Registry methods whose handler needs a server feature the method registry
/// does not mirror, paired with whether this build compiled it.
const BUILD_GATED_OPS: &[(&str, bool)] = &[("ResourceStatsPage", cfg!(feature = "cost"))];

/// Methods advertised only when the persistence backend declares support; the
/// `append_native_*` helpers own both the lists and the runtime condition.
fn backend_gated_ops() -> Vec<&'static str> {
    let mut ops = Vec::new();
    append_native_resource_ops(&mut ops, true);
    append_native_capacity_ops(&mut ops, true);
    append_native_work_item_ops(&mut ops, true);
    ops
}

/// Every method this build's registry (`eg_capabilities::method_descriptors`,
/// the one the dispatcher is policy-checked against) serves unconditionally:
/// the registry minus backend-gated methods (appended by their own condition),
/// methods whose server feature is absent, and contract-wave methods whose
/// handler has not landed. Never a hand list, so a served method cannot be
/// missing from `client.supports(..)`.
fn registry_served_ops() -> Vec<&'static str> {
    let backend_gated = backend_gated_ops();
    eg_capabilities::method_descriptors()
        .map(|descriptor| descriptor.id.as_str())
        .filter(|id| !backend_gated.contains(id) && !build_absent(id))
        .filter(|id| !crate::server::contract_wave::PENDING_METHODS.contains(id))
        .collect()
}

fn build_absent(id: &str) -> bool {
    BUILD_GATED_OPS
        .iter()
        .any(|(gated, compiled)| *gated == id && !compiled)
}

#[cfg(test)]
mod health_ops_tests {
    use super::*;
    use crate::server::contract_wave::PENDING_METHODS;

    /// R5 (au-core): `AgentComponent` was served but absent from the old hand
    /// list, so `client.supports("AgentComponent")` answered false.
    #[test]
    fn served_registry_methods_are_advertised() {
        let served = registry_served_ops();
        for id in [
            "AgentComponent",
            "ConnectorPack",
            "CommitWorkItemResult",
            "Health",
        ] {
            assert!(served.contains(&id), "{id} is served but not advertised");
        }
    }

    /// Every withholding list names real registry methods (a stale or
    /// misspelt entry would silently advertise nothing and withhold nothing),
    /// and nothing withheld is advertised.
    #[test]
    fn withheld_methods_are_registry_methods_and_never_advertised() {
        let registry: Vec<&str> = eg_capabilities::method_descriptors()
            .map(|descriptor| descriptor.id.as_str())
            .collect();
        let served = registry_served_ops();
        let build_gated = BUILD_GATED_OPS.iter().map(|(id, _)| *id);
        for id in backend_gated_ops()
            .into_iter()
            .chain(PENDING_METHODS.iter().copied())
        {
            assert!(registry.contains(&id), "{id} is not a registry method");
            assert!(!served.contains(&id), "{id} is withheld but advertised");
        }
        for id in build_gated {
            assert!(registry.contains(&id), "{id} is not a registry method");
            assert_eq!(served.contains(&id), !build_absent(id), "{id}");
        }
    }
}
