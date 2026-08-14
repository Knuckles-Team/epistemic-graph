//! Streaming / CDC / subscriptions / triggers handler (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230,
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
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::{check_graph_access, CarrierAuthority, GraphReadAuthority};
use crate::wire::{CdcEvent, ContinuousAgg, ContinuousQuerySpec, WatchBatch};

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
async fn seed_value(
    state: &Arc<RwLock<ServerState>>,
    authority: &GraphReadAuthority,
    spec: &ContinuousQuerySpec,
) -> f64 {
    let core = {
        let s = state.read().await;
        match s.registry.get(&spec.graph).map(|e| e.core.clone()) {
            Some(c) => c,
            None => return 0.0,
        }
    };
    let core = authority.project_core(&core);
    // Matching node rows: by label if set, else every visible node.
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
                eg_types::msgpack::decode_property_value(blob)
                    .ok()
                    .and_then(|v| v.get(field).and_then(|f| f.as_f64()))
                    .unwrap_or(0.0)
            })
            .sum(),
    }
}

async fn authorize_graph(
    state: &Arc<RwLock<ServerState>>,
    authority: &CarrierAuthority,
    graph: &str,
    access: AccessLevel,
) -> Result<(), String> {
    let s = state.read().await;
    let entry = s
        .registry
        .get(graph)
        .ok_or_else(|| format!("Graph '{graph}' not found"))?;
    check_graph_access(
        &s.isolation,
        Some(authority.agent_id()),
        graph,
        entry.graph_type,
        entry.owner.as_deref(),
        access,
    )
}

fn owned_name(authority: &CarrierAuthority, domain: &str, name: &str) -> String {
    format!("{domain}:{}:{name}", authority.owner_scope())
}

fn owned_prefix(authority: &CarrierAuthority, domain: &str) -> String {
    format!("{domain}:{}:", authority.owner_scope())
}

/// Keep only an event image visible to this actor. Ownership-changing updates may
/// expose the before image to the old owner and the after image to the new owner,
/// never both merely because one side was authorized.
fn sanitize_event(authority: &GraphReadAuthority, mut event: CdcEvent) -> Option<CdcEvent> {
    let before_visible = event.had_before && authority.can_see_blob(&event.before);
    let after_visible = event.had_after && authority.can_see_blob(&event.after);
    if !before_visible {
        event.had_before = false;
        event.before.clear();
    }
    if !after_visible {
        event.had_after = false;
        event.after.clear();
    }
    (before_visible || after_visible).then_some(event)
}

fn sanitize_watch(authority: &GraphReadAuthority, batch: WatchBatch) -> WatchBatch {
    WatchBatch {
        events: batch
            .events
            .into_iter()
            .filter_map(|event| sanitize_event(authority, event))
            .collect(),
        next_seq: batch.next_seq,
        gap: batch.gap,
        watermark: batch.watermark,
        head_seq: batch.head_seq,
        epoch: batch.epoch,
    }
}

/// Handle the streaming methods. Returns `Err(method)` for any non-streaming method so
/// the dispatch chain falls through — though dispatch only routes streaming methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    carrier: &CarrierAuthority,
    read_authority: &GraphReadAuthority,
    method: Method,
) -> Result<Response, Method> {
    // Raw hub sequence numbers, long-poll wakeups and aggregate through-seq
    // cursors reveal the existence/rate of filtered rows even when their payloads
    // are projected away.  The hub has no per-event actor-stable cursor ownership,
    // so active RLS fails this whole carrier closed except for an explicit admin.
    if read_authority.is_active() && !carrier.is_admin() {
        return Ok(Response::err(
            req_id,
            "ACCESS_DENIED: streaming cursors have no actor-stable ownership under active RLS",
        ));
    }
    match method {
        Method::CdcRead {
            graph,
            from_seq,
            limit,
        } => {
            if let Err(error) = authorize_graph(state, carrier, &graph, AccessLevel::Read).await {
                return Ok(Response::err(req_id, error));
            }
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            // B-8: `read` now returns a typed `CdcReadResult` carrying
            // `gap`/`watermark`/`head_seq`/`epoch` instead of a bare `Vec<CdcEvent>`
            // (which made a genuinely-caught-up cursor and a silently-fallen-off-the-
            // ring one indistinguishable `[]`) — sanitize only the events (a `gap`
            // result's `events` is always empty already) and pass the rest through.
            let mut result = hub.read(&graph, from_seq, limit);
            result.events = result
                .events
                .into_iter()
                .filter_map(|event| sanitize_event(read_authority, event))
                .collect();
            Ok(Response::ok(req_id, ResultPayload::raw(&result)))
        }

        Method::RegisterContinuousQuery { name, spec_msgpack } => {
            let spec: ContinuousQuerySpec = match eg_types::msgpack::decode_bounded(
                &spec_msgpack,
                eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 50_000, 64),
            ) {
                Ok(s) => s,
                Err(_) => {
                    return Ok(Response::err(
                        req_id,
                        "invalid or over-complex continuous query specification",
                    ))
                }
            };
            if let Err(error) =
                authorize_graph(state, carrier, &spec.graph, AccessLevel::Read).await
            {
                return Ok(Response::err(req_id, error));
            }
            let initial = seed_value(state, read_authority, &spec).await;
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            hub.register_query(owned_name(carrier, "cq", &name), spec, initial);
            Ok(Response::ok(req_id, ResultPayload::String(name)))
        }

        Method::ReadContinuousQuery { name } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            let storage_name = owned_name(carrier, "cq", &name);
            let Some(spec) = hub.query_spec(&storage_name) else {
                return Ok(Response::err(
                    req_id,
                    format!("continuous query '{name}' not found"),
                ));
            };
            if let Err(error) =
                authorize_graph(state, carrier, &spec.graph, AccessLevel::Read).await
            {
                return Ok(Response::err(req_id, error));
            }
            let value = seed_value(state, read_authority, &spec).await;
            Ok(match hub.read_query(&storage_name) {
                Some(mut result) => {
                    result.name = name;
                    result.value = value;
                    Response::ok(req_id, ResultPayload::raw(&result))
                }
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
                ResultPayload::Bool(hub.drop_query(&owned_name(carrier, "cq", &name))),
            ))
        }

        Method::Watch {
            graph,
            from_seq,
            label,
            timeout_ms,
        } => {
            if let Err(error) = authorize_graph(state, carrier, &graph, AccessLevel::Read).await {
                return Ok(Response::err(req_id, error));
            }
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
            // First pass: anything already pending since the cursor? B-8: `gap` is
            // surfaced IMMEDIATELY, never masked behind a long-poll wait — waiting
            // could let unrelated fresh writes numerically "resolve" the
            // `from_seq > head_seq` check without the cursor's original history
            // ever having been recovered, turning an explicit signal back into a
            // silent one.
            let batch =
                sanitize_watch(read_authority, hub.watch_batch(&graph, from_seq, &label, 0));
            if batch.gap || !batch.events.is_empty() {
                return Ok(Response::ok(req_id, ResultPayload::raw(&batch)));
            }
            // Nothing yet — long-poll: await the next change up to timeout_ms, then
            // return whatever arrived (possibly still empty on timeout). One
            // Request → one Response; the client re-issues Watch to keep tailing.
            let wait = Duration::from_millis(if timeout_ms == 0 { 0 } else { timeout_ms });
            if !wait.is_zero() {
                let _ = tokio::time::timeout(wait, notified).await;
            }
            let batch =
                sanitize_watch(read_authority, hub.watch_batch(&graph, from_seq, &label, 0));
            Ok(Response::ok(req_id, ResultPayload::raw(&batch)))
        }

        Method::RegisterTrigger {
            name,
            graph,
            label,
            op,
            action_msgpack,
        } => {
            if let Err(error) = authorize_graph(state, carrier, &graph, AccessLevel::Read).await {
                return Ok(Response::err(req_id, error));
            }
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            hub.register_trigger(
                owned_name(carrier, "trigger", &name),
                graph,
                label,
                op,
                action_msgpack,
            );
            Ok(Response::ok(req_id, ResultPayload::String(name)))
        }

        Method::DropTrigger { name } => {
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::Bool(hub.drop_trigger(&owned_name(carrier, "trigger", &name))),
            ))
        }

        Method::ListTriggers { graph } => {
            if let Err(error) = authorize_graph(state, carrier, &graph, AccessLevel::Read).await {
                return Ok(Response::err(req_id, error));
            }
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            let prefix = owned_prefix(carrier, "trigger");
            let mut triggers = hub.list_triggers(&graph);
            triggers.retain_mut(|trigger| {
                let Some(display) = trigger.name.strip_prefix(&prefix) else {
                    return false;
                };
                trigger.name = display.to_string();
                // The hub's raw counter includes invisible events; never expose it.
                trigger.fire_count = 0;
                true
            });
            Ok(Response::ok(req_id, ResultPayload::raw(&triggers)))
        }

        Method::FiredTriggers {
            graph,
            from_seq,
            limit,
        } => {
            if let Err(error) = authorize_graph(state, carrier, &graph, AccessLevel::Read).await {
                return Ok(Response::err(req_id, error));
            }
            let hub = match hub_of(state, req_id).await {
                Ok(h) => h,
                Err(r) => return Ok(r),
            };
            let prefix = owned_prefix(carrier, "trigger");
            // B-8 follow-up: `fired` now returns a typed `FiredTriggersResult`
            // (`gap`/`watermark`/`head_seq`/`epoch`) instead of a bare
            // `Vec<FiredAction>` — a `gap` result's `fired` is always empty already,
            // so the visibility filter below is a no-op for it.
            let mut result = hub.fired(&graph, from_seq, limit);
            result.fired.retain_mut(|action| {
                let Some(display) = action.trigger.strip_prefix(&prefix) else {
                    return false;
                };
                let visible = hub
                    .read(&graph, action.change_seq, 1)
                    .events
                    .into_iter()
                    .next()
                    .and_then(|event| sanitize_event(read_authority, event))
                    .is_some();
                if visible {
                    action.trigger = display.to_string();
                }
                visible
            });
            Ok(Response::ok(req_id, ResultPayload::raw(&result)))
        }

        other => Err(other),
    }
}
