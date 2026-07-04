//! WASM-sandboxed UDF handlers (CONCEPT:EG-KG.query.rowset-execution).
//!
//! `RegisterUdf` compiles + caches an agent-supplied WebAssembly module in the
//! process-global [`eg_wasm::UdfRegistry`] on `ServerState`; `RunUdf` runs a cached
//! UDF SANDBOXED (fuel + memory limits, NO host capabilities) over an opaque byte
//! payload. The compile/run is CPU-bound, so it runs on the blocking pool off the
//! reactor (a fuel-killed infinite loop therefore never stalls the runtime).

use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};

/// Try to handle a WASM-UDF method. `Ok(resp)` = handled; `Err(method)` = not mine.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::RegisterUdf { id, wasm } => {
            let registry = {
                let s = state.read().await;
                s.udf_registry.clone()
            };
            // Compile off-reactor (cranelift codegen is CPU-bound).
            let id_for_task = id.clone();
            let res = tokio::task::spawn_blocking(move || {
                registry.register(&id_for_task, &wasm, eg_wasm::UdfLimits::default())
            })
            .await;
            Ok(match res {
                Ok(Ok(())) => Response::ok(req_id, ResultPayload::String(id)),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(join) => Response::err(req_id, format!("RegisterUdf task error: {join}")),
            })
        }
        Method::RunUdf { id, input } => {
            let registry = {
                let s = state.read().await;
                s.udf_registry.clone()
            };
            // Run off-reactor: a fuel-killed infinite loop traps here without stalling
            // the Tokio runtime. The sandbox enforces fuel + memory + no-host-caps.
            let res = tokio::task::spawn_blocking(move || registry.run(&id, &input)).await;
            Ok(match res {
                Ok(Ok(out)) => Response::ok(req_id, ResultPayload::Raw(out)),
                Ok(Err(e)) => Response::err(req_id, e.to_string()),
                Err(join) => Response::err(req_id, format!("RunUdf task error: {join}")),
            })
        }
        other => Err(other),
    }
}
