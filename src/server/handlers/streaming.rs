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
use eg_types::result_contract::messaging as results;

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

fn sum_numeric_field(rows: &[(String, Vec<u8>)], field: &str) -> f64 {
    let mut total = 0.0;
    for (_, blob) in rows {
        if let Ok(value) = eg_types::msgpack::decode_property_value(blob) {
            if let Some(number) = value.get(field).and_then(|field| field.as_f64()) {
                total += number;
            }
        }
    }
    total
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
        ContinuousAgg::Sum { field } => sum_numeric_field(&rows, field),
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

async fn authorized_hub(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    carrier: &CarrierAuthority,
    graph: &str,
) -> Result<Arc<crate::server::cdc::CdcHub>, Response> {
    if let Err(error) = authorize_graph(state, carrier, graph, AccessLevel::Read).await {
        return Err(Response::err(req_id, error));
    }
    hub_of(state, req_id).await
}

struct StreamingRequest<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    carrier: &'a CarrierAuthority,
    read_authority: &'a GraphReadAuthority,
}

impl<'a> StreamingRequest<'a> {
    async fn cdc_read(&self, graph: String, from_seq: u64, limit: u32) -> Response {
        let hub = match authorized_hub(self.state, self.req_id, self.carrier, &graph).await {
            Ok(hub) => hub,
            Err(response) => return response,
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
            .filter_map(|event| sanitize_event(self.read_authority, event))
            .collect();
        Response::ok(
            self.req_id,
            ResultPayload::of_ref::<results::CdcRead>(&result),
        )
    }

    async fn register_query(&self, name: String, spec_msgpack: Vec<u8>) -> Response {
        let spec: ContinuousQuerySpec = match eg_types::msgpack::decode_bounded(
            &spec_msgpack,
            eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 50_000, 64),
        ) {
            Ok(spec) => spec,
            Err(_) => {
                return Response::err(
                    self.req_id,
                    "invalid or over-complex continuous query specification",
                )
            }
        };
        let hub = match authorized_hub(self.state, self.req_id, self.carrier, &spec.graph).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        let initial = seed_value(self.state, self.read_authority, &spec).await;
        hub.register_query(owned_name(self.carrier, "cq", &name), spec, initial);
        Response::ok(
            self.req_id,
            ResultPayload::scalar::<results::RegisterContinuousQuery>(name),
        )
    }

    async fn read_query(&self, name: String) -> Response {
        let hub = match hub_of(self.state, self.req_id).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        let storage_name = owned_name(self.carrier, "cq", &name);
        let Some(spec) = hub.query_spec(&storage_name) else {
            return Response::err(self.req_id, format!("continuous query '{name}' not found"));
        };
        if let Err(error) =
            authorize_graph(self.state, self.carrier, &spec.graph, AccessLevel::Read).await
        {
            return Response::err(self.req_id, error);
        }
        let value = seed_value(self.state, self.read_authority, &spec).await;
        match hub.read_query(&storage_name) {
            Some(mut result) => {
                result.name = name;
                result.value = value;
                Response::ok(
                    self.req_id,
                    ResultPayload::of_ref::<results::ReadContinuousQuery>(&result),
                )
            }
            None => Response::err(self.req_id, format!("continuous query '{name}' not found")),
        }
    }

    async fn drop_query(&self, name: String) -> Response {
        let hub = match hub_of(self.state, self.req_id).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        Response::ok(
            self.req_id,
            ResultPayload::scalar::<results::DropContinuousQuery>(hub.drop_query(&owned_name(
                self.carrier,
                "cq",
                &name,
            ))),
        )
    }

    async fn watch(
        &self,
        graph: String,
        from_seq: u64,
        label: String,
        timeout_ms: u64,
    ) -> Response {
        let hub = match authorized_hub(self.state, self.req_id, self.carrier, &graph).await {
            Ok(hub) => hub,
            Err(response) => return response,
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
        let batch = sanitize_watch(
            self.read_authority,
            hub.watch_batch(&graph, from_seq, &label, 0),
        );
        if batch.gap || !batch.events.is_empty() {
            return Response::ok(self.req_id, ResultPayload::of_ref::<results::Watch>(&batch));
        }
        // Nothing yet — long-poll: await the next change up to timeout_ms, then
        // return whatever arrived (possibly still empty on timeout). One
        // Request → one Response; the client re-issues Watch to keep tailing.
        let wait = Duration::from_millis(timeout_ms);
        if !wait.is_zero() {
            let _ = tokio::time::timeout(wait, notified).await;
        }
        let batch = sanitize_watch(
            self.read_authority,
            hub.watch_batch(&graph, from_seq, &label, 0),
        );
        Response::ok(self.req_id, ResultPayload::of_ref::<results::Watch>(&batch))
    }

    async fn register_trigger(
        &self,
        name: String,
        graph: String,
        label: String,
        op: String,
        action_msgpack: Vec<u8>,
    ) -> Response {
        let hub = match authorized_hub(self.state, self.req_id, self.carrier, &graph).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        hub.register_trigger(
            owned_name(self.carrier, "trigger", &name),
            graph,
            label,
            op,
            action_msgpack,
        );
        Response::ok(
            self.req_id,
            ResultPayload::scalar::<results::RegisterTrigger>(name),
        )
    }

    async fn drop_trigger(&self, name: String) -> Response {
        let hub = match hub_of(self.state, self.req_id).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        Response::ok(
            self.req_id,
            ResultPayload::scalar::<results::DropTrigger>(hub.drop_trigger(&owned_name(
                self.carrier,
                "trigger",
                &name,
            ))),
        )
    }

    async fn list_triggers(&self, graph: String) -> Response {
        let hub = match authorized_hub(self.state, self.req_id, self.carrier, &graph).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        let prefix = owned_prefix(self.carrier, "trigger");
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
        Response::ok(
            self.req_id,
            ResultPayload::of_ref::<results::ListTriggers>(&triggers),
        )
    }

    async fn fired_triggers(&self, graph: String, from_seq: u64, limit: u32) -> Response {
        let hub = match authorized_hub(self.state, self.req_id, self.carrier, &graph).await {
            Ok(hub) => hub,
            Err(response) => return response,
        };
        let prefix = owned_prefix(self.carrier, "trigger");
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
                .and_then(|event| sanitize_event(self.read_authority, event))
                .is_some();
            if visible {
                action.trigger = display.to_string();
            }
            visible
        });
        Response::ok(
            self.req_id,
            ResultPayload::of_ref::<results::FiredTriggers>(&result),
        )
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
    let request = StreamingRequest {
        state,
        req_id,
        carrier,
        read_authority,
    };
    match method {
        Method::CdcRead {
            graph,
            from_seq,
            limit,
        } => Ok(request.cdc_read(graph, from_seq, limit).await),
        Method::RegisterContinuousQuery { name, spec_msgpack } => {
            Ok(request.register_query(name, spec_msgpack).await)
        }
        Method::ReadContinuousQuery { name } => Ok(request.read_query(name).await),
        Method::DropContinuousQuery { name } => Ok(request.drop_query(name).await),
        Method::Watch {
            graph,
            from_seq,
            label,
            timeout_ms,
        } => Ok(request.watch(graph, from_seq, label, timeout_ms).await),
        Method::RegisterTrigger {
            name,
            graph,
            label,
            op,
            action_msgpack,
        } => Ok(request
            .register_trigger(name, graph, label, op, action_msgpack)
            .await),
        Method::DropTrigger { name } => Ok(request.drop_trigger(name).await),
        Method::ListTriggers { graph } => Ok(request.list_triggers(graph).await),
        Method::FiredTriggers {
            graph,
            from_seq,
            limit,
        } => Ok(request.fired_triggers(graph, from_seq, limit).await),
        other => Err(other),
    }
}
