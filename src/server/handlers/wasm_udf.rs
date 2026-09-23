//! WASM-sandboxed UDF handlers (CONCEPT:EG-KG.query.rowset-execution).
//!
//! `RegisterUdf` compiles + caches an agent-supplied WebAssembly module in the
//! owner-scoped [`crate::server::udf_catalog::UdfCatalog`] on `ServerState`, under the
//! caller's VERIFIED owner — tenant+principal (EH-374); `RunUdf` runs the caller's own
//! cached UDF SANDBOXED (fuel + memory limits, NO host capabilities) over an opaque byte
//! payload. Another principal's id resolves as unregistered and cannot be shadowed. The
//! compile/run is CPU-bound, so it runs on the blocking pool off the reactor (a
//! fuel-killed infinite loop therefore never stalls the runtime).

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;

/// The refusal for a UDF method that reached dispatch without a verified carrier.
fn carrier_denied(req_id: u64, method: &str) -> Response {
    crate::metrics::access_denied();
    Response::err(
        req_id,
        format!("ACCESS_DENIED: {method} requires a verified tenant carrier"),
    )
}

/// Try to handle a WASM-UDF method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    carrier: Option<&CarrierAuthority>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::RegisterUdf { id, wasm } => {
            let Some(owner) = carrier.cloned() else {
                return Ok(carrier_denied(req_id, "RegisterUdf"));
            };
            let registry = state.read().await.udf_registry.clone();
            // Compile off-reactor (cranelift codegen is CPU-bound).
            let id_for_task = id.clone();
            let res = tokio::task::spawn_blocking(move || {
                registry.register(&owner, &id_for_task, &wasm, eg_wasm::UdfLimits::default())
            })
            .await;
            Ok(match res {
                Ok(Ok(())) => Response::ok(
                    req_id,
                    ResultPayload::scalar::<eg_types::result_contract::cluster::RegisterUdf>(id),
                ),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(join) => Response::err(req_id, format!("RegisterUdf task error: {join}")),
            })
        }
        Method::RunUdf { id, input } => {
            let Some(caller) = carrier.cloned() else {
                return Ok(carrier_denied(req_id, "RunUdf"));
            };
            let registry = state.read().await.udf_registry.clone();
            // Run off-reactor: a fuel-killed infinite loop traps here without stalling
            // the Tokio runtime. The sandbox enforces fuel + memory + no-host-caps.
            let res = tokio::task::spawn_blocking(move || registry.run(&caller, &id, &input)).await;
            Ok(match res {
                Ok(Ok(out)) => Response::ok(
                    req_id,
                    ResultPayload::of_encoded::<eg_types::result_contract::compute::RunUdf>(out),
                ),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(join) => Response::err(req_id, format!("RunUdf task error: {join}")),
            })
        }
        other => Err(other),
    }
}
