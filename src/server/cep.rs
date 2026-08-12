//! Live CEP standing-query surface (CONCEPT:EG-KG.query.protocol-types) — the SERVER-side transport that
//! wires the shipped, runtime-tested `eg_stream::live::CepEngine` (CONCEPT:EG-KG.query.pipelined-execution) to a
//! live event feed and a client-facing register/poll surface.
//!
//! ## What EG-088 already gives us (and what was left)
//! `eg_stream::live` is a complete standing-query engine: [`CepEngine`] owns N standing
//! queries, [`CepEngine::ingest`] fans one [`Event`] to every query's incremental NFA and
//! publishes matches per-query over `tokio::sync::broadcast` (drop-oldest + lag-count
//! backpressure), and [`CepEngine::spawn_source`] drains a `broadcast::Receiver<Event>`
//! "event bus" into `ingest`. Its own docs flagged the SERVER wiring as the remaining
//! step, exactly like `eg_graphql::LiveQuery`: eg-stream is a pure leaf crate that must
//! not depend on eg-core, so the SERVER owns (1) the adapter turning each committed change
//! into an [`Event`] on a `broadcast::Sender<Event>`, and (2) the subscription route.
//! This module IS that wiring — the engine is untouched.
//!
//! ## The CDC → Event adapter
//! The engine's reactive substrate is the EG-064 / KG-2.229 CDC hub
//! ([`crate::server::cdc::CdcHub`]): every durable single-row mutation emits an ordered
//! [`crate::wire::CdcEvent`]. This [`CepSurface`] holds a `broadcast::Sender<Event>` the
//! [`CepEngine`] drains; the CDC hub calls [`CepSurface::feed_change`] on each emit, mapping
//! the `CdcEvent` to an [`Event`] whose `key` is the node/edge label (the useful CEP
//! discriminator — e.g. `Alert` node added then `Ack` node added) and whose `ts` is a
//! surface-global monotonic clock (so events across graphs form ONE ts-ordered stream, the
//! order the NFA steppers assume). The surface is created LAZILY on the first subscribe, so
//! a CDC feed with no CEP subscriber pays nothing (and construction — which spawns the drain
//! task — always happens inside the async dispatch handler, where a runtime exists).
//!
//! ## The subscription surface (transport-compatible)
//! Rather than a new streaming frame / socket, EG-299 rides the SAME one-Request →
//! one-Response transport every other reactive op uses (`CdcRead`/`Watch`/…):
//!
//! * `CepSubscribe { pattern }` → register the pattern as a standing query, returns a
//!   subscription id.
//! * `CepPoll { sub_id, timeout_ms }` → LONG-POLL (like `Watch`) for the matches pushed
//!   since the last poll — returns immediately if any are buffered, else awaits the next
//!   up to `timeout_ms`. The client re-polls to keep tailing.
//! * `CepUnsubscribe { sub_id }` → drop the standing query + its subscriber.
//!
//! Non-SQL / streaming by nature, so — exactly like the CDC methods — it is served ONLY
//! over RPC dispatch, never pgwire (the "gate non-SQL" path).
//!
//! Gated `all(streaming, stream)`: the CDC hub (`streaming`) feeds the live NFA engine
//! (`stream`, the only thing that pulls eg-stream's tokio). A `pi` build (streaming, no
//! stream) compiles none of this and the `Cep*` methods fall to the dispatch not-available
//! catch-all.
//!
//! ## Push extension: the broker bridge (opt-in, feature `broker`)
//! `CepPoll` above is a pull surface (long-poll). W4.10/M6 adds a genuine PUSH surface by
//! reusing the engine's OWN broker (EG-275..284) rather than inventing a new transport:
//! when `EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE` names a target, every match a standing query
//! detects is ALSO published — topic-routed, routing key = the subscription id — onto that
//! exchange in `__commons__`. Any already-connected AMQP/MQTT/STOMP consumer (the three wire
//! adapters' own poll-driven push pumps) is then pushed the match with no further client
//! action, and any RPC client can equally `BrokerConsume` it. This is purely additive over
//! `CepSubscribe`'s registration path (see [`forward_to_broker_if_configured`]) — it never
//! touches [`CdcHub::emit`](crate::server::cdc::CdcHub::emit) or [`CepSurface::feed_change`],
//! so the write-path cost of this extension, whether armed or not, is exactly zero: unset
//! (the default) means the whole section below is inert and no forwarder task ever exists.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use eg_stream::live::{CepEngine, CepSubscription, DEFAULT_MATCH_BUFFER};
use eg_stream::{AttrPredicate, CepPattern, Event, EventMatcher, Match, Window};

use super::state::ServerState;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::wire::{CdcEvent, CdcKind, CepMatcherSpec, CepNodeSpec, CepPatternSpec, CepWindowSpec};

/// Bounded capacity of the CDC → CEP event bus. A CEP engine that falls this far behind
/// the write firehose drops the OLDEST events (and `spawn_source` records the lag via
/// `CepEngine::source_lagged`) rather than stalling the writer — the same graceful
/// backpressure the per-subscription match buffer gives on the read side.
const CEP_EVENT_BUS_CAP: usize = 4096;

/// The live CEP standing-query surface (CONCEPT:EG-KG.query.protocol-types): the `eg_stream` [`CepEngine`]
/// plus the CDC → [`Event`] bus that feeds it and the per-subscription receivers the
/// `CepPoll` handler drains. Cheap to `Arc`-share; owned lazily by the [`CdcHub`]
/// (created on the first subscribe).
pub struct CepSurface {
    /// The shipped live standing-query engine (EG-088) — untouched; we only feed + read it.
    engine: Arc<CepEngine>,
    /// Server-held per-subscription receivers, keyed by the engine's standing-query id
    /// (which is the wire `sub_id`). `Arc<AsyncMutex<…>>` so `CepPoll` can clone the handle
    /// out and long-poll WITHOUT holding a DashMap shard guard across the `.await`.
    subs: DashMap<u64, Arc<AsyncMutex<CepSubscription>>>,
    /// Surface-global monotonic event clock: the `ts` fed to every CEP [`Event`]. A single
    /// counter across all graphs makes the CDC changes ONE ts-ordered stream (what the NFA
    /// steppers assume) irrespective of the per-graph CDC `seq`.
    clock: AtomicU64,
    /// The CDC → CEP event bus sender. Held so the channel stays open for the drain task's
    /// lifetime (a `broadcast` closes when the last sender drops).
    bus: broadcast::Sender<Event>,
    /// The `spawn_source` drain task (bus → `engine.ingest`). Aborted when the surface drops.
    _pump: JoinHandle<()>,
}

impl CepSurface {
    /// Build the surface: a fresh [`CepEngine`], the CDC → [`Event`] bus, and the drain task
    /// that feeds the engine from it. MUST be called inside a Tokio runtime (it spawns the
    /// drain task) — it always is, because the surface is created lazily from the async
    /// subscribe handler.
    pub fn new() -> Arc<CepSurface> {
        let engine = Arc::new(CepEngine::new());
        let (bus, rx) = broadcast::channel::<Event>(CEP_EVENT_BUS_CAP);
        let pump = engine.clone().spawn_source(rx);
        Arc::new(CepSurface {
            engine,
            subs: DashMap::new(),
            clock: AtomicU64::new(0),
            bus,
            _pump: pump,
        })
    }

    /// Adapter (CONCEPT:EG-KG.query.protocol-types): map one committed [`CdcEvent`] to a CEP [`Event`] and
    /// publish it on the bus the engine drains. `key` = the change's label if present (the
    /// useful CEP discriminator) else the op kind (`add`/`remove`/`update`); the op, graph,
    /// and ids ride in `attrs` for attribute predicates. Publishing with no live drain
    /// receiver is impossible (the surface owns the pump for its whole life); a bus that has
    /// no room drops the oldest event (recorded as source lag) — never blocks the writer.
    pub fn feed_change(&self, ev: &CdcEvent) {
        let op = kind_op(&ev.kind);
        let ts = self.clock.fetch_add(1, Ordering::Relaxed);
        let key = if ev.label.is_empty() {
            op.to_string()
        } else {
            ev.label.clone()
        };
        let mut attrs = serde_json::Map::new();
        attrs.insert("graph".to_string(), ev.graph.clone().into());
        attrs.insert("op".to_string(), op.into());
        attrs.insert("node_id".to_string(), ev.node_id.clone().into());
        if !ev.target_id.is_empty() {
            attrs.insert("target_id".to_string(), ev.target_id.clone().into());
        }
        if !ev.label.is_empty() {
            attrs.insert("label".to_string(), ev.label.clone().into());
        }
        // Ignore the "no receivers" case — the pump is always live for the surface's life.
        let _ = self.bus.send(Event { ts, key, attrs });
    }

    /// Register a standing query for `pattern` over `window` (CONCEPT:EG-KG.query.protocol-types), keeping the
    /// engine's [`CepSubscription`] server-side keyed by its id. Returns that id (the wire
    /// `sub_id`).
    pub fn register(&self, pattern: &CepPattern, window: Window, buffer: usize) -> u64 {
        let sub = self.engine.register(pattern, window, buffer);
        let id = sub.id;
        self.subs.insert(id, Arc::new(AsyncMutex::new(sub)));
        id
    }

    /// Long-poll subscription `sub_id` for pushed matches (CONCEPT:EG-KG.query.protocol-types): drain everything
    /// already buffered; if none and `timeout_ms > 0`, await the next match up to the
    /// timeout, then drain any that arrived alongside it. A lagging poller silently skips the
    /// dropped-oldest matches (broadcast `Lagged`) and keeps going. `Err` iff `sub_id` is
    /// unknown (dropped / never registered).
    pub async fn poll(&self, sub_id: u64, timeout_ms: u64) -> Result<Vec<Match>, String> {
        // Clone the Arc handle out and DROP the DashMap shard guard before awaiting, so a
        // long poll never blocks `CepUnsubscribe` (or another key in the same shard).
        let sub = self
            .subs
            .get(&sub_id)
            .map(|r| r.clone())
            .ok_or_else(|| format!("CEP subscription {sub_id} not found"))?;
        let mut sub = sub.lock().await;

        let mut out = Vec::new();
        drain_ready(&mut sub, &mut out);
        if out.is_empty() && timeout_ms > 0 {
            let wait = std::time::Duration::from_millis(timeout_ms);
            // `Ok(Ok(m))` = a match arrived; anything else (Lagged/Closed while awaiting, or
            // the timeout elapsing) → return whatever we have.
            if let Ok(Ok(m)) = tokio::time::timeout(wait, sub.recv()).await {
                out.push(m);
                drain_ready(&mut sub, &mut out);
            }
        }
        Ok(out)
    }

    /// Drop the standing query with `sub_id` + its server-held receiver (CONCEPT:EG-KG.query.protocol-types).
    /// Returns whether it existed.
    pub fn unsubscribe(&self, sub_id: u64) -> bool {
        let existed = self.subs.remove(&sub_id).is_some();
        if existed {
            self.engine.unregister(sub_id);
        }
        existed
    }

    /// An INDEPENDENT resubscribed receiver on `sub_id`'s match stream (its own lag
    /// accounting), for a forwarder that must not disturb `CepPoll`'s buffered drain —
    /// the broker-push bridge below is the first caller. `None` if `sub_id` is unknown
    /// (dropped / never registered). Mirrors `eg_stream::live::CepSubscription::resubscribe`'s
    /// documented fan-out contract: many independent consumers may watch one standing query.
    pub async fn resubscribe(&self, sub_id: u64) -> Option<CepSubscription> {
        let sub = self.subs.get(&sub_id)?.clone();
        let sub = sub.lock().await;
        Some(sub.resubscribe())
    }

    /// How many standing queries are currently registered (test/introspection helper).
    #[cfg(test)]
    pub fn sub_count(&self) -> usize {
        self.subs.len()
    }
}

/// Drain every match currently buffered on `sub` into `out`, skipping any lag gap and
/// stopping at empty/closed.
fn drain_ready(sub: &mut CepSubscription, out: &mut Vec<Match>) {
    loop {
        match sub.try_recv() {
            Ok(m) => out.push(m),
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Empty)
            | Err(broadcast::error::TryRecvError::Closed) => break,
        }
    }
}

/// Map a CDC kind to the CEP event op string (mirrors the CDC hub's trigger `op`).
fn kind_op(kind: &CdcKind) -> &'static str {
    match kind {
        CdcKind::AddNode | CdcKind::AddEdge => "add",
        CdcKind::RemoveNode | CdcKind::RemoveEdge => "remove",
        CdcKind::UpdateNode => "update",
    }
}

// ── Pure-serde `CepPatternSpec` (eg-types) → `eg_stream` conversion ──────────────
// The wire DTOs are Pi-safe (no eg-stream dep); this seam turns them into the real NFA
// pattern behind the `stream` gate — the same mapping `eg_plan::exec` does for the batch
// `Op::Cep`, kept local so the CEP surface stays confined to this module.

fn window_from_spec(w: CepWindowSpec) -> Window {
    match w {
        CepWindowSpec::Sliding { size } => Window::Sliding { size },
        CepWindowSpec::Tumbling { size } => Window::Tumbling { size },
    }
}

fn matcher_from_spec(m: &CepMatcherSpec) -> EventMatcher {
    use crate::wire::CepAttrPredSpec;
    let preds = m
        .preds
        .iter()
        .map(|p| match p {
            CepAttrPredSpec::Eq { field, value } => AttrPredicate::Eq {
                field: field.clone(),
                value: value.clone(),
            },
            CepAttrPredSpec::Gt { field, value } => AttrPredicate::Gt {
                field: field.clone(),
                value: *value,
            },
            CepAttrPredSpec::Lt { field, value } => AttrPredicate::Lt {
                field: field.clone(),
                value: *value,
            },
            CepAttrPredSpec::Exists { field } => AttrPredicate::Exists {
                field: field.clone(),
            },
        })
        .collect();
    EventMatcher {
        key: m.key.clone(),
        preds,
    }
}

fn pattern_from_spec(p: &CepNodeSpec) -> CepPattern {
    match p {
        CepNodeSpec::Sequence(matchers) => {
            CepPattern::Sequence(matchers.iter().map(matcher_from_spec).collect())
        }
        CepNodeSpec::Within { within, pattern } => CepPattern::Within {
            within: *within,
            pattern: Box::new(pattern_from_spec(pattern)),
        },
        CepNodeSpec::Absence { a, b, within } => CepPattern::Absence {
            a: matcher_from_spec(a),
            b: matcher_from_spec(b),
            within: *within,
        },
    }
}

// ── CEP → broker push bridge (opt-in, W4.10/M6) ───────────────────────────────────
// See the module doc's "Push extension" section. Everything here runs off
// `CepSubscribe`'s registration path only; NONE of it is reachable from
// `CdcHub::emit`/`CepSurface::feed_change`, so the per-write cost of this whole
// section — armed or not — is zero (structural: a `git diff` of `cdc.rs` for this
// change is empty).

/// Env var naming the broker exchange every standing query's matches are ALSO
/// published to. Unset/blank ⇒ no broker forwarding — `CepPoll` remains the only
/// delivery path, byte-for-byte the pre-W4.10 behavior.
#[cfg(feature = "broker")]
const CEP_BROKER_EXCHANGE_ENV: &str = "EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE";

/// The graph whose broker hosts forwarded CEP matches. CEP patterns are cross-graph by
/// design (no mandatory graph in the wire contract; the surface is admin-only), so
/// forwarded matches live on the one graph guaranteed to exist — the same commons/
/// control graph the broker's own docs already use for cross-cutting state.
#[cfg(feature = "broker")]
const CEP_BROKER_GRAPH: &str = "__commons__";

/// Read + trim the configured exchange name.
#[cfg(feature = "broker")]
fn cep_broker_exchange() -> Option<String> {
    parse_cep_broker_exchange(std::env::var(CEP_BROKER_EXCHANGE_ENV).ok().as_deref())
}

/// Pure parse step, split out from [`cep_broker_exchange`] so the gate logic is
/// testable without touching process-global env state (tests in this crate run
/// concurrently, so mutating a real env var would race).
#[cfg(feature = "broker")]
fn parse_cep_broker_exchange(raw: Option<&str>) -> Option<String> {
    let trimmed = raw?.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Encode + publish one [`Match`] to the broker (CONCEPT:EG-KG.compute.message-broker-exchanges), topic-routed by
/// the subscription id so a consumer can bind to one subscription's matches
/// (`bind_queue(exchange, q, "<sub_id>")`) or every match on the exchange (`"#"`).
/// Declaring the exchange is idempotent (a no-op once it already exists as `Topic`); a
/// kind collision or an encode failure is logged and dropped — the forwarder never
/// panics or blocks the match stream over a broker-side problem.
#[cfg(feature = "broker")]
fn publish_match_to_broker(core: &crate::graph::GraphCore, exchange: &str, sub_id: u64, m: &Match) {
    if let Err(error) =
        crate::broker::declare_exchange(core, exchange, crate::broker::ExchangeKind::Topic)
    {
        tracing::warn!(exchange, sub_id, %error, "CEP broker forwarder: exchange declare failed");
        return;
    }
    let Ok(payload) = rmp_serde::to_vec_named(m) else {
        tracing::warn!(
            exchange,
            sub_id,
            "CEP broker forwarder: match encode failed"
        );
        return;
    };
    let routing_key = sub_id.to_string();
    let delivered = crate::broker::publish(core, exchange, &routing_key, &payload);
    tracing::debug!(
        exchange,
        sub_id,
        match_events = m.events.len(),
        delivered,
        "CEP match pushed to broker"
    );
}

/// Drain `rx` for the lifetime of standing query `sub_id`, publishing each match to
/// `exchange` on `core`. Ends when the standing query is unregistered (the one
/// `broadcast::Sender` its `StandingQuery` owns drops, so `recv` reports `Closed`). A
/// forwarder that falls behind drops the oldest matches and logs the count — it never
/// blocks CEP ingestion, matching the poll surface's own drop-oldest contract.
#[cfg(feature = "broker")]
fn spawn_cep_broker_forwarder(
    core: Arc<crate::graph::GraphCore>,
    exchange: String,
    sub_id: u64,
    mut rx: CepSubscription,
) {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(m) => publish_match_to_broker(&core, &exchange, sub_id, &m),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        sub_id,
                        exchange,
                        lagged = n,
                        "CEP broker forwarder dropped the oldest matches"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

/// If `EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE` is configured, start forwarding `sub_id`'s
/// matches to it. A no-op (no task spawned, no registry read) when unset — the gate
/// this whole extension's zero-cost-when-off claim rests on.
#[cfg(feature = "broker")]
async fn forward_to_broker_if_configured(
    state: &Arc<RwLock<ServerState>>,
    surface: &Arc<CepSurface>,
    sub_id: u64,
) {
    if let Some(exchange) = cep_broker_exchange() {
        forward_to_broker(state, surface, sub_id, &exchange).await;
    }
}

/// Resolve `__commons__`, take an independent resubscribed receiver on `sub_id`, and
/// spawn the forwarder. Split from [`forward_to_broker_if_configured`] so a test can
/// drive it directly with an explicit exchange (no env var, so no cross-test races).
#[cfg(feature = "broker")]
async fn forward_to_broker(
    state: &Arc<RwLock<ServerState>>,
    surface: &Arc<CepSurface>,
    sub_id: u64,
    exchange: &str,
) {
    let core = {
        let s = state.read().await;
        s.registry
            .get(CEP_BROKER_GRAPH)
            .map(|entry| entry.core.clone())
    };
    let Some(core) = core else {
        tracing::warn!(
            exchange,
            sub_id,
            graph = CEP_BROKER_GRAPH,
            "CEP broker forwarder: commons graph unavailable"
        );
        return;
    };
    let Some(rx) = surface.resubscribe(sub_id).await else {
        return;
    };
    spawn_cep_broker_forwarder(core, exchange.to_string(), sub_id, rx);
}

/// Reach the lazily-created CEP surface off the CDC hub (creating it on first use), or an
/// ERROR response if the engine somehow booted without a CDC hub.
async fn surface_of(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
) -> Result<Arc<CepSurface>, Response> {
    let s = state.read().await;
    match &s.cdc {
        Some(hub) => Ok(hub.cep_surface()),
        None => Err(Response::err(req_id, "streaming/CDC not configured")),
    }
}

/// Handle the live CEP standing-query methods (CONCEPT:EG-KG.query.protocol-types). Returns `Err(method)` for
/// any non-CEP method so the dispatch chain falls through — though dispatch only routes the
/// `Cep*` methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Response, Method> {
    // CEP patterns have no mandatory graph in the wire contract, so they cannot
    // be row-projected safely. Preserve the protocol only for an explicitly
    // verified cluster administrator; ordinary tenants fail closed.
    if let Err(error) = authority.require_admin("CEP subscriptions") {
        return Ok(Response::err(req_id, error));
    }
    match method {
        Method::CepSubscribe {
            pattern_msgpack,
            buffer,
        } => {
            let spec: CepPatternSpec = match eg_types::msgpack::decode_bounded(
                &pattern_msgpack,
                eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 50_000, 64),
            ) {
                Ok(s) => s,
                Err(_) => return Ok(Response::err(req_id, "invalid or over-complex CEP pattern")),
            };
            let surface = match surface_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            let pattern = pattern_from_spec(&spec.pattern);
            let window = window_from_spec(spec.window);
            let buf = if buffer == 0 {
                DEFAULT_MATCH_BUFFER
            } else {
                buffer as usize
            };
            let id = surface.register(&pattern, window, buf);
            // Opt-in push extension (W4.10/M6): if configured, forward this standing
            // query's matches to the broker too. No-op (nothing read, nothing spawned)
            // when `EPISTEMIC_GRAPH_CEP_BROKER_EXCHANGE` is unset — see the module doc.
            #[cfg(feature = "broker")]
            forward_to_broker_if_configured(state, &surface, id).await;
            Ok(Response::ok(req_id, ResultPayload::Count(id)))
        }

        Method::CepPoll { sub_id, timeout_ms } => {
            let surface = match surface_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            Ok(match surface.poll(sub_id, timeout_ms).await {
                Ok(matches) => Response::ok(req_id, ResultPayload::raw(&matches)),
                Err(e) => Response::err(req_id, e),
            })
        }

        Method::CepUnsubscribe { sub_id } => {
            let surface = match surface_of(state, req_id).await {
                Ok(s) => s,
                Err(r) => return Ok(r),
            };
            Ok(Response::ok(
                req_id,
                ResultPayload::Bool(surface.unsubscribe(sub_id)),
            ))
        }

        other => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cdc(graph: &str, kind: CdcKind, node: &str, label: &str) -> CdcEvent {
        CdcEvent {
            seq: 0,
            graph: graph.to_string(),
            kind,
            node_id: node.to_string(),
            target_id: String::new(),
            label: label.to_string(),
            before: Vec::new(),
            after: Vec::new(),
            had_before: false,
            had_after: false,
        }
    }

    // A single-step "Alert" sequence: every add of an `Alert`-labelled node is a match.
    fn alert_pattern() -> CepPattern {
        CepPattern::Sequence(vec![EventMatcher {
            key: Some("Alert".to_string()),
            preds: Vec::new(),
        }])
    }

    #[tokio::test]
    async fn eg299_register_feed_poll_pushes_match() {
        let surface = CepSurface::new();
        let sub = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        assert_eq!(surface.sub_count(), 1);

        // A non-Alert change → no match.
        surface.feed_change(&cdc("g", CdcKind::AddNode, "n0", "Doc"));
        // An Alert change → the standing query fires; the drain task must propagate it, so
        // long-poll for it.
        surface.feed_change(&cdc("g", CdcKind::AddNode, "n1", "Alert"));

        let matches = surface.poll(sub, 1000).await.expect("known sub");
        assert_eq!(matches.len(), 1, "exactly the Alert add matched");
        assert_eq!(matches[0].events[0].key, "Alert");
        assert_eq!(matches[0].events[0].attrs.get("node_id").unwrap(), "n1");
    }

    #[tokio::test]
    async fn eg299_multiple_subscribers_each_get_the_match() {
        let surface = CepSurface::new();
        // Two INDEPENDENT standing queries on the same pattern — each is fanned the event.
        let a = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        let b = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        assert_ne!(a, b);
        assert_eq!(surface.sub_count(), 2);

        surface.feed_change(&cdc("g", CdcKind::AddNode, "n1", "Alert"));

        let ma = surface.poll(a, 1000).await.expect("sub a");
        let mb = surface.poll(b, 1000).await.expect("sub b");
        assert_eq!(ma.len(), 1, "subscriber a saw the match");
        assert_eq!(mb.len(), 1, "subscriber b saw the match");
    }

    #[tokio::test]
    async fn eg299_unsubscribe_drops_the_query() {
        let surface = CepSurface::new();
        let sub = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        assert!(surface.unsubscribe(sub), "existing sub removed");
        assert_eq!(surface.sub_count(), 0);
        assert!(!surface.unsubscribe(sub), "second remove is a no-op");
        // Polling a dropped subscription is a typed error, not a panic.
        assert!(surface.poll(sub, 0).await.is_err());
    }

    #[tokio::test]
    async fn eg299_poll_empty_returns_promptly() {
        let surface = CepSurface::new();
        let sub = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        // Nothing fed → an empty, non-error poll (zero timeout ⇒ immediate).
        let matches = surface.poll(sub, 0).await.expect("known sub");
        assert!(matches.is_empty());
    }

    #[test]
    fn eg299_pattern_spec_conversion_roundtrips_shape() {
        // Within-wrapped sequence with an attribute predicate → the eg_stream pattern.
        let spec = CepNodeSpec::Within {
            within: 5,
            pattern: Box::new(CepNodeSpec::Sequence(vec![
                CepMatcherSpec {
                    key: Some("Alert".to_string()),
                    preds: vec![crate::wire::CepAttrPredSpec::Gt {
                        field: "sev".to_string(),
                        value: 3.0,
                    }],
                },
                CepMatcherSpec {
                    key: Some("Ack".to_string()),
                    preds: Vec::new(),
                },
            ])),
        };
        match pattern_from_spec(&spec) {
            CepPattern::Within { within, pattern } => {
                assert_eq!(within, 5);
                match *pattern {
                    CepPattern::Sequence(ref m) => assert_eq!(m.len(), 2),
                    _ => panic!("expected inner Sequence"),
                }
            }
            _ => panic!("expected Within"),
        }
    }

    // ── W4.10/M6: CEP → broker push bridge ────────────────────────────────────────

    #[cfg(feature = "broker")]
    #[test]
    fn cep_broker_exchange_gate_unset_or_blank_disables_forwarding() {
        assert_eq!(parse_cep_broker_exchange(None), None);
        assert_eq!(parse_cep_broker_exchange(Some("")), None);
        assert_eq!(parse_cep_broker_exchange(Some("   ")), None);
        assert_eq!(
            parse_cep_broker_exchange(Some("  cep-matches  ")),
            Some("cep-matches".to_string())
        );
    }

    /// A minimal `ServerState` for the broker-forward round-trip. Mirrors the
    /// `test_state()` fixture the wire-adapter test modules (mqtt_wire et al.) already
    /// use; every optional/feature-gated field is `None`/empty so it compiles under any
    /// feature combination that also has `broker` on.
    #[cfg(feature = "broker")]
    fn test_state() -> Arc<RwLock<ServerState>> {
        use crate::channels::ChannelManager;
        use crate::isolation::IsolationLayer;
        use crate::registry::GraphRegistry;
        use dashmap::DashMap;
        use tokio::sync::Semaphore;
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: "test".to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    /// ACCEPTANCE (W4.10/M6): a Sequence pattern registered over live mutations fires a
    /// push event end-to-end over the broker — a matching CDC stream reaches the bound
    /// broker queue; a non-matching one produces nothing.
    #[cfg(feature = "broker")]
    #[tokio::test]
    async fn cep_match_reaches_the_broker_queue_end_to_end() {
        let core = crate::graph::GraphCore::new();
        let exchange = "cep-test-exchange";
        crate::broker::declare_exchange(&core, exchange, crate::broker::ExchangeKind::Topic)
            .unwrap();
        // A wildcard binding: one queue watching every subscription's matches.
        crate::broker::bind_queue(&core, exchange, "watchers", "#");

        let surface = CepSurface::new();
        let sub_id = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        let mut forward_rx = surface.resubscribe(sub_id).await.expect("known sub");

        // Non-matching event: the pattern never completes, so nothing is ever sent on
        // the match stream.
        surface.feed_change(&cdc("g", CdcKind::AddNode, "n0", "Doc"));
        // Matching event: the Sequence completes → exactly one Match is published.
        surface.feed_change(&cdc("g", CdcKind::AddNode, "n1", "Alert"));

        let m = tokio::time::timeout(std::time::Duration::from_secs(2), forward_rx.recv())
            .await
            .expect("a match arrives before the timeout")
            .expect("no lag/close on a fresh resubscribe");
        publish_match_to_broker(&core, exchange, sub_id, &m);

        let claim = serde_json::json!({"status": "claimed"})
            .as_object()
            .unwrap()
            .clone();
        let got = core.claim_next_fields(&crate::broker::queue_msg_label("watchers"), &claim);
        assert!(got.is_some(), "the match must reach the bound broker queue");
        let (_, props) = got.unwrap();
        assert_eq!(
            props.get("routing_key").and_then(|v| v.as_str()),
            Some(sub_id.to_string().as_str()),
            "routed by the subscription id, so a consumer can bind to just this subscription"
        );
        let hexed = props.get("payload").and_then(|v| v.as_str()).unwrap();
        let payload = crate::broker::hex_decode(hexed).unwrap();
        let decoded: Match = rmp_serde::from_slice(&payload).unwrap();
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].key, "Alert");

        // The non-matching Doc event never produced a Match, so nothing else is queued.
        assert!(
            core.claim_next_fields(&crate::broker::queue_msg_label("watchers"), &claim)
                .is_none(),
            "a non-matching event must not push anything"
        );
    }

    /// The full async wiring — `forward_to_broker` resolves `__commons__`, resubscribes,
    /// and spawns the live forwarder task — delivers a match without the test driving
    /// the forwarder loop by hand (unlike the test above, which calls
    /// `publish_match_to_broker` directly after awaiting the resubscribed receiver).
    #[cfg(feature = "broker")]
    #[tokio::test]
    async fn forward_to_broker_spawns_a_live_forwarder_that_delivers_matches() {
        let state = test_state();
        let exchange = "cep-live-exchange";
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        crate::broker::declare_exchange(&core, exchange, crate::broker::ExchangeKind::Topic)
            .unwrap();
        crate::broker::bind_queue(&core, exchange, "live", "#");

        let surface = CepSurface::new();
        let sub_id = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        forward_to_broker(&state, &surface, sub_id, exchange).await;

        surface.feed_change(&cdc("g", CdcKind::AddNode, "n1", "Alert"));

        let claim = serde_json::json!({"status": "claimed"})
            .as_object()
            .unwrap()
            .clone();
        let label = crate::broker::queue_msg_label("live");
        // Bounded retry: the forwarder runs on its own tokio task, so delivery is
        // asynchronous — not a fixed sleep, so this is fast on a healthy box and never
        // hangs on a slow/contended one (a genuine miss still fails after the bound).
        let mut got = None;
        for _ in 0..200 {
            if let Some(hit) = core.claim_next_fields(&label, &claim) {
                got = Some(hit);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let (_, props) = got.expect("the live forwarder must deliver the match");
        let hexed = props.get("payload").and_then(|v| v.as_str()).unwrap();
        let decoded: Match =
            rmp_serde::from_slice(&crate::broker::hex_decode(hexed).unwrap()).unwrap();
        assert_eq!(decoded.events[0].key, "Alert");
    }

    /// Zero-cost-when-off (structural, mirroring W3.6's cost gate): with the env var
    /// unset, `forward_to_broker_if_configured` never reads the registry or
    /// resubscribes — it returns having done nothing observable. Combined with the fact
    /// that this whole bridge is reachable ONLY from `CepSubscribe`'s registration path
    /// (never from `CdcHub::emit`/`feed_change`, unlike a write-path hook), the write
    /// path carries zero added cost whether or not this feature is armed.
    #[cfg(feature = "broker")]
    #[tokio::test]
    async fn forward_to_broker_if_configured_is_a_noop_when_env_is_unset() {
        assert!(
            std::env::var(CEP_BROKER_EXCHANGE_ENV).is_err(),
            "test assumes the CI/dev environment never sets this var"
        );
        let state = test_state();
        let surface = CepSurface::new();
        let sub_id = surface.register(
            &alert_pattern(),
            Window::Sliding { size: 0 },
            DEFAULT_MATCH_BUFFER,
        );
        // Must return promptly and must not disturb the surface: `CepPoll`'s own
        // subscription is unaffected (still exactly one, still pollable).
        forward_to_broker_if_configured(&state, &surface, sub_id).await;
        assert_eq!(surface.sub_count(), 1);
        surface.feed_change(&cdc("g", CdcKind::AddNode, "n1", "Alert"));
        let matches = surface.poll(sub_id, 1000).await.expect("known sub");
        assert_eq!(matches.len(), 1, "CepPoll still works exactly as before");
    }
}
