//! Live CEP standing-query surface (CONCEPT:EG-299) — the SERVER-side transport that
//! wires the shipped, runtime-tested `eg_stream::live::CepEngine` (CONCEPT:EG-088) to a
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
//!   * `CepSubscribe { pattern }` → register the pattern as a standing query, returns a
//!     subscription id.
//!   * `CepPoll { sub_id, timeout_ms }` → LONG-POLL (like `Watch`) for the matches pushed
//!     since the last poll — returns immediately if any are buffered, else awaits the next
//!     up to `timeout_ms`. The client re-polls to keep tailing.
//!   * `CepUnsubscribe { sub_id }` → drop the standing query + its subscriber.
//! Non-SQL / streaming by nature, so — exactly like the CDC methods — it is served ONLY
//! over RPC dispatch, never pgwire (the "gate non-SQL" path).
//!
//! Gated `all(streaming, stream)`: the CDC hub (`streaming`) feeds the live NFA engine
//! (`stream`, the only thing that pulls eg-stream's tokio). A `pi` build (streaming, no
//! stream) compiles none of this and the `Cep*` methods fall to the dispatch not-available
//! catch-all.

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
use crate::wire::{CdcEvent, CdcKind, CepMatcherSpec, CepNodeSpec, CepPatternSpec, CepWindowSpec};

/// Bounded capacity of the CDC → CEP event bus. A CEP engine that falls this far behind
/// the write firehose drops the OLDEST events (and `spawn_source` records the lag via
/// `CepEngine::source_lagged`) rather than stalling the writer — the same graceful
/// backpressure the per-subscription match buffer gives on the read side.
const CEP_EVENT_BUS_CAP: usize = 4096;

/// The live CEP standing-query surface (CONCEPT:EG-299): the `eg_stream` [`CepEngine`]
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

    /// Adapter (CONCEPT:EG-299): map one committed [`CdcEvent`] to a CEP [`Event`] and
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

    /// Register a standing query for `pattern` over `window` (CONCEPT:EG-299), keeping the
    /// engine's [`CepSubscription`] server-side keyed by its id. Returns that id (the wire
    /// `sub_id`).
    pub fn register(&self, pattern: &CepPattern, window: Window, buffer: usize) -> u64 {
        let sub = self.engine.register(pattern, window, buffer);
        let id = sub.id;
        self.subs.insert(id, Arc::new(AsyncMutex::new(sub)));
        id
    }

    /// Long-poll subscription `sub_id` for pushed matches (CONCEPT:EG-299): drain everything
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
            match tokio::time::timeout(wait, sub.recv()).await {
                Ok(Ok(m)) => {
                    out.push(m);
                    drain_ready(&mut sub, &mut out);
                }
                // Lagged/Closed while awaiting, or the timeout elapsed → return what we have.
                Ok(Err(_)) | Err(_) => {}
            }
        }
        Ok(out)
    }

    /// Drop the standing query with `sub_id` + its server-held receiver (CONCEPT:EG-299).
    /// Returns whether it existed.
    pub fn unsubscribe(&self, sub_id: u64) -> bool {
        let existed = self.subs.remove(&sub_id).is_some();
        if existed {
            self.engine.unregister(sub_id);
        }
        existed
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

/// Handle the live CEP standing-query methods (CONCEPT:EG-299). Returns `Err(method)` for
/// any non-CEP method so the dispatch chain falls through — though dispatch only routes the
/// `Cep*` methods here.
pub async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::CepSubscribe {
            pattern_msgpack,
            buffer,
        } => {
            let spec: CepPatternSpec = match rmp_serde::from_slice(&pattern_msgpack) {
                Ok(s) => s,
                Err(e) => {
                    return Ok(Response::err(req_id, format!("invalid pattern_msgpack: {e}")))
                }
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
        let sub = surface.register(&alert_pattern(), Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
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
        let a = surface.register(&alert_pattern(), Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
        let b = surface.register(&alert_pattern(), Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
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
        let sub = surface.register(&alert_pattern(), Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
        assert!(surface.unsubscribe(sub), "existing sub removed");
        assert_eq!(surface.sub_count(), 0);
        assert!(!surface.unsubscribe(sub), "second remove is a no-op");
        // Polling a dropped subscription is a typed error, not a panic.
        assert!(surface.poll(sub, 0).await.is_err());
    }

    #[tokio::test]
    async fn eg299_poll_empty_returns_promptly() {
        let surface = CepSurface::new();
        let sub = surface.register(&alert_pattern(), Window::Sliding { size: 0 }, DEFAULT_MATCH_BUFFER);
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
}
