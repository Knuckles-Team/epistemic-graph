//! Pure-compute data-science ops (CONCEPT:EG-KG.compute.rust-native-training-loss): sklearn-parity estimators,
//! primitives, and training kernels. Stateless — no graph core, runs inline.

// See finance.rs: the Result router moves the large `Method` enum by value on the
// fall-through path; boxing the Err would allocate per non-DS request.
#![allow(clippy::result_large_err)]

mod routes;

use crate::protocol::{Method, Response};

/// Handle a `Ds*` method. `Err(method)` hands a non-datascience method back to the
/// dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention — server dispatch convention)
pub(crate) fn try_handle(req_id: u64, method: Method) -> Result<Response, Method> {
    routes::try_handle(req_id, method)
}
