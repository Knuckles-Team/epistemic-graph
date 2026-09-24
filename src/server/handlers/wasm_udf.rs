//! WASM-sandboxed UDF handlers (CONCEPT:EG-KG.query.rowset-execution).
//!
//! `RegisterUdf` compiles + caches an agent-supplied WebAssembly module in the
//! owner-scoped [`crate::server::udf_catalog::UdfCatalog`] on `ServerState`, under the
//! caller's VERIFIED owner — tenant+principal (EH-374); `RunUdf` runs the caller's own
//! cached UDF SANDBOXED (fuel + memory limits, NO host capabilities) over an opaque byte
//! payload. Another principal's id resolves as unregistered and cannot be shadowed. The
//! compile/run is CPU-bound, so it runs on the blocking pool off the reactor (a
//! fuel-killed infinite loop therefore never stalls the runtime).

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::OwnerScopedCall;

/// Try to handle a WASM-UDF method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    call: OwnerScopedCall<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = call.req_id;
    match method {
        Method::RegisterUdf { id, wasm } => {
            let owner = match call.owner("RegisterUdf") {
                Ok(owner) => owner,
                Err(refusal) => return Ok(refusal),
            };
            let registry = call.state.read().await.udf_registry.clone();
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
            let caller = match call.owner("RunUdf") {
                Ok(caller) => caller,
                Err(refusal) => return Ok(refusal),
            };
            let registry = call.state.read().await.udf_registry.clone();
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
