//! Pure-compute quantitative-finance ops (CONCEPT:AU-KG.memory.mementified-context): portfolio optimization,
//! risk, regimes, signals, microstructure/market-making, sizing, backtest
//! validation, state-space, derivatives. Stateless — no graph core, runs inline.

// The Result-based router moves `Method` by value through the fall-through chain
// (Err = "not mine, try next"). `Method` is a large enum, but the dispatcher already
// moves it by value everywhere; boxing the Err would add a heap allocation on every
// non-finance request (the common path), so we keep the move and scope the lint.
#![allow(clippy::result_large_err)]

mod route_families;
mod routes;

use crate::protocol::{Method, Response};

/// Handle a `Finance*` method. `Err(method)` hands a non-finance method back to the
/// dispatcher (routing fall-through). (CONCEPT:EG-KG.query.dispatch-convention — server dispatch convention)
pub(crate) fn try_handle(req_id: u64, method: Method) -> Result<Response, Method> {
    routes::try_handle(req_id, method)
}
