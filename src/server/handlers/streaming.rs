//! Streaming / CDC / subscriptions / triggers handler (CONCEPT:KG-2.229/230,
//! feature `streaming`).
//!
//! Owns the `// ── Streaming ──` protocol section. These are STATEFUL (they drive the
//! [`CdcHub`] on `ServerState`), so like the txn/timeseries handlers they take
//! `state`. The hub is fed from the dispatch write-side-effect block (every durable
//! mutation emits a `CdcEvent`); this handler is the READ + REGISTER surface over it.
//!
//! Transport-compatible: every op is one Request → one Response over the existing
//! socket. `CdcRead`/`Watch`/`FiredTriggers` are cursor-driven (a `from_seq` the
//! consumer advances); `Watch` long-polls — it awaits the per-graph `Notify` up to
//! `timeout_ms` for the first change, then returns whatever arrived (NOT a streaming
//! frame side-channel).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use super::super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
use crate::wire::{ContinuousAgg, ContinuousQuerySpec};

/// Pull the CDC hub, or an ERROR response if the engine booted without one (only if a
/// future build path leaves it `None` — `streaming` builds always construct it).
async fn hub_of(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
) -> Result<Arc<crate::server::cdc::CdcHub>, Response> {
    let s = state.read().await;
    match &s.cdc {
        Some(h) => Ok(h.clone()),
        None => Err(Response::err(req_id, "streaming/CDC not configured")),
    }
}

/// Compute the seed value for a continuous query from the graph's CURRENT state, so
/// the view is correct at registration (then deltas maintain it). `count` = matching
/// node count; `sum:<field>` = sum of the numeric field over matching nodes.
async fn seed_value(state: &Arc<RwLock<ServerState>>, spec: &ContinuousQuerySpec) -> f64 {
    let core = {
        let s = state.read().await;
        match s.registry.get(&spec.graph).map(|e| e.core.clone()) {
            Some(c) => c,
            None => return 0.0,
        }
    };
    // Matching node rows: by label if set, else every node.
    let rows: Vec<(String, Vec<u8>)> = if spec.label.is_empty() {
        core.get_nodes()
    } else {
        core.get_nodes_by_label(&spec.label, 0)
    };
    match &spec.agg {
        ContinuousAgg::Count => rows.len() as f64,
        ContinuousAgg::Sum { field } => rows
            .iter()
            .map(|(_, blob)| {
                rmp_serde::from_slice::<serde_json::Value>(blob)
                    .ok()
                    .and_then(|v| v.get(field).and_then(|f| f.as_f64()))
                    .unwrap_or(0.0)
            })
            .sum(),
    }
}

/// Handle the streaming methods. Returns `Err(method)` for any non-streaming method so
/// the dispatch chain falls through — though dispatch only routes streaming methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::CdcRead {
            graph,
            from_seq,
            limit,
        } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(match hub.read(&graph, from_seq, limit) {
                Ok(events) => Response::ok(req_id, ResultPayload::raw(&events)),
                Err(e) => Response::err(req_id, e),
            })
        }

        Method::RegisterContinuousQuery { name, spec_msgpack } => {
            let spec: ContinuousQuerySpec = match rmp_serde::from_slice(&spec_msgpack) {
                Ok(s) => s,
                Err(e) => return Ok(Response::err(req_id, format!("invalid spec_msgpack: {e}"))),
            };
            let initial = seed_value(state, &spec).await;
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            hub.register_query(name.clone(), spec, initial);
            Ok(Response::ok(req_id, ResultPayload::String(name)))
        }

        Method::ReadContinuousQuery { name } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(match hub.read_query(&name) {
                Some(r) => Response::ok(req_id, ResultPayload::raw(&r)),
                None => Response::err(req_id, format!("continuous query '{name}' not found")),
            })
        }

        Method::DropContinuousQuery { name } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::Bool(hub.drop_query(&name)),
            ))
        }

        Method::Watch {
            graph,
            from_seq,
            label,
            timeout_ms,
        } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            // Arm the change-notification future BEFORE the first pending check so a
            // write landing in the gap between the check and the await still wakes us
            // (`Notify::notified()` captures any `notify_waiters` after its creation) —
            // closing the lost-wakeup race. The future is enabled (registers the waiter)
            // on first poll, so pin it and enable it up front.
            let notify = hub.notifier(&graph);
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            // First pass: anything already pending since the cursor?
            let batch = match hub.watch_batch(&graph, from_seq, &label, 0) {
                Ok(b) => b,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            if !batch.events.is_empty() {
                return Ok(Response::ok(req_id, ResultPayload::raw(&batch)));
            }
            // Nothing yet — long-poll: await the next change up to timeout_ms, then
            // return whatever arrived (possibly still empty on timeout). One
            // Request → one Response; the client re-issues Watch to keep tailing.
            let wait = Duration::from_millis(if timeout_ms == 0 { 0 } else { timeout_ms });
            if !wait.is_zero() {
                let _ = tokio::time::timeout(wait, notified).await;
            }
            let batch = match hub.watch_batch(&graph, from_seq, &label, 0) {
                Ok(b) => b,
                Err(e) => return Ok(Response::err(req_id, e)),
            };
            Ok(Response::ok(req_id, ResultPayload::raw(&batch)))
        }

        Method::RegisterTrigger {
            name,
            graph,
            label,
            op,
            action_msgpack,
        } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            hub.register_trigger(name.clone(), graph, label, op, action_msgpack);
            Ok(Response::ok(req_id, ResultPayload::String(name)))
        }

        Method::DropTrigger { name } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::Bool(hub.drop_trigger(&name)),
            ))
        }

        Method::ListTriggers { graph } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::raw(&hub.list_triggers(&graph)),
            ))
        }

        Method::FiredTriggers {
            graph,
            from_seq,
            limit,
        } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::raw(&hub.fired(&graph, from_seq, limit)),
            ))
        }

        other => Err(other),
    }
}
