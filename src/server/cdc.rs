//! Change-Data-Capture hub + continuous queries + subscriptions/triggers
//! (CONCEPT:KG-2.229/230, feature `streaming`).
//!
//! Reactivity layered OVER the engine's existing per-graph durable change record
//! (the ledger): the dispatch shell already records every durable mutation
//! centrally; here that SAME point also emits an ordered, cursor-addressable
//! [`CdcEvent`] into a per-graph in-memory feed. From that ONE feed three surfaces
//! are driven without any new transport — they all ride the existing
//! one-Response-per-Request socket:
//!
//!   * **CDC feed** ([`CdcHub::read`]) — `CdcRead { from_seq }` tails the ordered
//!     changes since a cursor (poll/stream). The foundation for incremental
//!     matviews, mirrors, and external sinks.
//!   * **Continuous queries** — a registered aggregate/filter view, maintained
//!     INCREMENTALLY on each emitted change (delta, not a re-run).
//!   * **Subscriptions / triggers** — `Watch` long-poll over a graph/label cursor,
//!     plus registered condition→action triggers whose firings are pollable.
//!
//! Pure-Rust, no heavy dep: a bounded `VecDeque` ring per graph + a `tokio::Notify`
//! for watch wakeups. Lean enough to fold into the Pi tier.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::wire::{
    CdcEvent, CdcKind, ContinuousAgg, ContinuousQueryResult, ContinuousQuerySpec, FiredAction,
    TriggerInfo, WatchBatch,
};

/// Default per-graph ring capacity (events retained for cursor reads). Overridable
/// with `EPISTEMIC_GRAPH_CDC_RING`. A cursor older than the trimmed window gets an
/// explicit "cursor behind retained window" error rather than silently skipping.
fn ring_cap() -> usize {
    std::env::var("EPISTEMIC_GRAPH_CDC_RING")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or(65_536)
}

/// Per-graph CDC state: a monotonic seq, the retained event ring, the fired-trigger
/// log, and a `Notify` so a `Watch` long-poll wakes the instant a change lands.
struct GraphFeed {
    /// Next seq to assign (== count of events ever emitted for this graph).
    next_seq: u64,
    /// Bounded ring of recent events (front = oldest retained).
    ring: VecDeque<CdcEvent>,
    /// Seq of the oldest event still retained (`ring.front().seq`); a `from_seq`
    /// below this is behind the window.
    oldest_seq: u64,
    /// Fired-trigger log (CONCEPT:KG-2.230), its own monotonic cursor.
    next_fire: u64,
    fired: VecDeque<FiredAction>,
    notify: Arc<Notify>,
    cap: usize,
}

impl GraphFeed {
    fn new(cap: usize) -> Self {
        GraphFeed {
            next_seq: 0,
            ring: VecDeque::new(),
            oldest_seq: 0,
            next_fire: 0,
            fired: VecDeque::with_capacity(64),
            notify: Arc::new(Notify::new()),
            cap,
        }
    }
}

/// A registered continuous query + its incrementally-maintained value.
struct ContinuousQuery {
    spec: ContinuousQuerySpec,
    value: f64,
    through_seq: u64,
}

/// A registered trigger.
#[derive(Clone)]
struct Trigger {
    name: String,
    graph: String,
    label: String,
    op: String,
    action: Vec<u8>,
    fire_count: u64,
}

/// The CDC hub on `ServerState`. One per engine; routes by graph name.
pub struct CdcHub {
    feeds: Mutex<HashMap<String, GraphFeed>>,
    queries: Mutex<HashMap<String, ContinuousQuery>>,
    triggers: Mutex<HashMap<String, Trigger>>,
    cap: usize,
}

impl Default for CdcHub {
    fn default() -> Self {
        Self::new()
    }
}

impl CdcHub {
    pub fn new() -> Self {
        CdcHub {
            feeds: Mutex::new(HashMap::new()),
            queries: Mutex::new(HashMap::new()),
            triggers: Mutex::new(HashMap::new()),
            cap: ring_cap(),
        }
    }

    /// Clone the per-graph `Notify` so a `Watch` can await the next change. Creates the
    /// feed if it does not yet exist (a watcher may subscribe before the first write).
    pub fn notifier(&self, graph: &str) -> Arc<Notify> {
        let mut feeds = self.feeds.lock();
        feeds
            .entry(graph.to_string())
            .or_insert_with(|| GraphFeed::new(self.cap))
            .notify
            .clone()
    }

    /// Emit one change into `graph`'s feed (CONCEPT:KG-2.229). Assigns the per-graph
    /// seq, pushes the ring (trimming the oldest past the cap), drives every continuous
    /// query + trigger that matches, then wakes watchers. Returns the assigned seq.
    pub fn emit(
        &self,
        graph: &str,
        kind: CdcKind,
        node_id: String,
        target_id: String,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    ) -> u64 {
        let label = extract_label(after.as_deref().or(before.as_deref()));
        let (seq, notify) = {
            let mut feeds = self.feeds.lock();
            let feed = feeds
                .entry(graph.to_string())
                .or_insert_with(|| GraphFeed::new(self.cap));
            let seq = feed.next_seq;
            feed.next_seq += 1;
            let event = CdcEvent {
                seq,
                graph: graph.to_string(),
                kind,
                node_id,
                target_id,
                label,
                had_before: before.is_some(),
                had_after: after.is_some(),
                before: before.unwrap_or_default(),
                after: after.unwrap_or_default(),
            };
            feed.ring.push_back(event.clone());
            if feed.ring.len() > feed.cap {
                feed.ring.pop_front();
                feed.oldest_seq = feed.ring.front().map(|e| e.seq).unwrap_or(seq);
            }
            // Drive views/triggers under no feed lock (separate locks) by cloning out.
            self.maintain(&event);
            self.fire_triggers(feed, &event);
            (seq, feed.notify.clone())
        };
        notify.notify_waiters();
        seq
    }

    /// Read the ordered feed for `graph` from `from_seq` (inclusive), capped at `limit`
    /// (0 ⇒ 1024). `Err` when `from_seq` is behind the retained window (the consumer
    /// must re-seed). An empty `Ok(vec)` means "caught up" (cursor at/after the head).
    pub fn read(&self, graph: &str, from_seq: u64, limit: u32) -> Result<Vec<CdcEvent>, String> {
        let cap = if limit == 0 { 1024 } else { limit as usize };
        let feeds = self.feeds.lock();
        let Some(feed) = feeds.get(graph) else {
            return Ok(Vec::new());
        };
        if from_seq < feed.oldest_seq {
            return Err(format!(
                "CDC cursor {from_seq} is behind the retained window (oldest seq {}); re-seed",
                feed.oldest_seq
            ));
        }
        Ok(feed
            .ring
            .iter()
            .filter(|e| e.seq >= from_seq)
            .take(cap)
            .cloned()
            .collect())
    }

    /// The current head seq (next to be assigned) for `graph` — `Watch`'s `next_seq`
    /// when no events matched.
    pub fn head_seq(&self, graph: &str) -> u64 {
        self.feeds
            .lock()
            .get(graph)
            .map(|f| f.next_seq)
            .unwrap_or(0)
    }

    /// Reset a graph's change feed (CONCEPT:KG-2.229): drop its retained ring + fired
    /// log and rewind the seq to 0. Called when the graph itself is wiped (`ClearGraph`)
    /// — the per-node changes are moot once the graph is empty, so a cleared graph
    /// starts a fresh feed (a consumer re-seeds from seq 0). Continuous queries over the
    /// graph reset to their empty-state value (their watermark rewinds with the feed).
    /// Watchers are woken so a blocked `Watch` returns promptly.
    pub fn reset_graph(&self, graph: &str) {
        let notify = {
            let mut feeds = self.feeds.lock();
            if let Some(feed) = feeds.get_mut(graph) {
                feed.ring.clear();
                feed.fired.clear();
                feed.next_seq = 0;
                feed.oldest_seq = 0;
                feed.next_fire = 0;
                Some(feed.notify.clone())
            } else {
                None
            }
        };
        for q in self.queries.lock().values_mut() {
            if q.spec.graph == graph {
                q.value = 0.0;
                q.through_seq = 0;
            }
        }
        if let Some(n) = notify {
            n.notify_waiters();
        }
    }

    /// Build a `Watch` batch: the matching changes since `from_seq` (filtered by
    /// `label`), and the cursor to resume from. Non-blocking — the handler does the
    /// `Notify`-await loop, calling this each wake.
    pub fn watch_batch(
        &self,
        graph: &str,
        from_seq: u64,
        label: &str,
        limit: u32,
    ) -> Result<WatchBatch, String> {
        let events: Vec<CdcEvent> = self
            .read(graph, from_seq, limit)?
            .into_iter()
            .filter(|e| label.is_empty() || e.label == label)
            .collect();
        let next_seq = events
            .last()
            .map(|e| e.seq + 1)
            .unwrap_or_else(|| from_seq.max(self.head_seq(graph)));
        Ok(WatchBatch { events, next_seq })
    }

    // ── Continuous queries (CONCEPT:KG-2.229) ────────────────────────────────

    /// Register (or replace) a continuous query, seeding its value from `initial`
    /// (the current matching state computed by the handler, so the view is correct
    /// from registration — not just from future deltas).
    pub fn register_query(&self, name: String, spec: ContinuousQuerySpec, initial: f64) {
        let through_seq = self.head_seq(&spec.graph);
        self.queries.lock().insert(
            name,
            ContinuousQuery {
                spec,
                value: initial,
                through_seq,
            },
        );
    }

    pub fn read_query(&self, name: &str) -> Option<ContinuousQueryResult> {
        self.queries
            .lock()
            .get(name)
            .map(|q| ContinuousQueryResult {
                name: name.to_string(),
                value: q.value,
                through_seq: q.through_seq,
            })
    }

    pub fn drop_query(&self, name: &str) -> bool {
        self.queries.lock().remove(name).is_some()
    }

    /// Fold one change into every continuous query it affects (the incremental step).
    fn maintain(&self, event: &CdcEvent) {
        let mut queries = self.queries.lock();
        for q in queries.values_mut() {
            if q.spec.graph != event.graph {
                continue;
            }
            // Only seqs the view hasn't folded in yet (registration sets a watermark).
            if event.seq < q.through_seq {
                continue;
            }
            if !q.spec.label.is_empty() && event.label != q.spec.label {
                q.through_seq = event.seq + 1;
                continue;
            }
            match &q.spec.agg {
                ContinuousAgg::Count => match event.kind {
                    CdcKind::AddNode => q.value += 1.0,
                    CdcKind::RemoveNode => q.value -= 1.0,
                    _ => {}
                },
                ContinuousAgg::Sum { field } => {
                    let old = if event.had_before {
                        field_num(&event.before, field)
                    } else {
                        0.0
                    };
                    let new = if event.had_after {
                        field_num(&event.after, field)
                    } else {
                        0.0
                    };
                    match event.kind {
                        CdcKind::AddNode => q.value += new,
                        CdcKind::RemoveNode => q.value -= old,
                        CdcKind::UpdateNode => q.value += new - old,
                        _ => {}
                    }
                }
            }
            q.through_seq = event.seq + 1;
        }
    }

    // ── Triggers / reactions (CONCEPT:KG-2.230) ──────────────────────────────

    pub fn register_trigger(
        &self,
        name: String,
        graph: String,
        label: String,
        op: String,
        action: Vec<u8>,
    ) {
        self.triggers.lock().insert(
            name.clone(),
            Trigger {
                name,
                graph,
                label,
                op,
                action,
                fire_count: 0,
            },
        );
    }

    pub fn drop_trigger(&self, name: &str) -> bool {
        self.triggers.lock().remove(name).is_some()
    }

    pub fn list_triggers(&self, graph: &str) -> Vec<TriggerInfo> {
        self.triggers
            .lock()
            .values()
            .filter(|t| t.graph == graph)
            .map(|t| TriggerInfo {
                name: t.name.clone(),
                graph: t.graph.clone(),
                label: t.label.clone(),
                op: t.op.clone(),
                fire_count: t.fire_count,
            })
            .collect()
    }

    /// Poll the fired-trigger log for `graph` from `from_seq` (inclusive).
    pub fn fired(&self, graph: &str, from_seq: u64, limit: u32) -> Vec<FiredAction> {
        let cap = if limit == 0 { 1024 } else { limit as usize };
        let feeds = self.feeds.lock();
        let Some(feed) = feeds.get(graph) else {
            return Vec::new();
        };
        feed.fired
            .iter()
            .filter(|f| f.fire_seq >= from_seq)
            .take(cap)
            .cloned()
            .collect()
    }

    /// Record a firing for every trigger matching this change. Runs under the feed
    /// lock (the `fired` log lives on the feed) so the firing's `fire_seq` is ordered.
    fn fire_triggers(&self, feed: &mut GraphFeed, event: &CdcEvent) {
        let op = kind_op(&event.kind);
        // Snapshot the matching triggers (separate lock) then record firings.
        let matched: Vec<Trigger> = {
            let mut triggers = self.triggers.lock();
            let mut out = Vec::new();
            for t in triggers.values_mut() {
                if t.graph != event.graph {
                    continue;
                }
                if !t.label.is_empty() && t.label != event.label {
                    continue;
                }
                if t.op != "any" && t.op != op {
                    continue;
                }
                t.fire_count += 1;
                out.push(t.clone());
            }
            out
        };
        for t in matched {
            let fire_seq = feed.next_fire;
            feed.next_fire += 1;
            feed.fired.push_back(FiredAction {
                fire_seq,
                trigger: t.name,
                graph: event.graph.clone(),
                change_seq: event.seq,
                node_id: event.node_id.clone(),
                action: t.action,
            });
            // Bound the fired log to the same cap as the ring.
            if feed.fired.len() > feed.cap {
                feed.fired.pop_front();
            }
        }
    }
}

// ── Dispatch glue: capture before/after + emit from a durable Method ──────────
//
// These translate ONE durable `Method` (the same set `is_durable_mutation` records)
// into a CDC emit, capturing the affected node/edge property pre-image BEFORE the
// write applies and the post-image AFTER. They read the `GraphCore` directly, so they
// work for BOTH the inline and the write-coalescer apply paths (the emit is off the
// returned Response in the dispatch shell, not inside any handler). `BatchUpdate` and
// `ClearGraph` are durable but multi-row / whole-graph; they emit nothing here (a
// consumer rebuilds from the ledger/checkpoint) — the single-row node/edge changes are
// the reactive unit. Returns the `Method`'s captured pre-image for the post-emit.

use crate::graph::GraphCore;
use crate::protocol::Method;

/// The pre-image captured before a durable mutation applies (the affected node/edge's
/// current property blob), threaded to [`emit_for_method`] after the write succeeds.
pub enum CdcPre {
    /// Not a single-row CDC-worthy change (BatchUpdate/ClearGraph/etc.) — skip emit.
    Skip,
    Node {
        node_id: String,
        before: Option<Vec<u8>>,
    },
    Edge {
        source_id: String,
        target_id: String,
        before: Option<Vec<u8>>,
    },
}

/// Capture the pre-image for a durable mutation against `core`, BEFORE it applies.
pub fn capture_before(core: &GraphCore, method: &Method) -> CdcPre {
    match method {
        Method::AddNode { node_id, .. }
        | Method::RemoveNode { node_id }
        | Method::CompareAndSetNodeFields { node_id, .. } => CdcPre::Node {
            node_id: node_id.clone(),
            before: core.get_node_properties(node_id),
        },
        Method::AddEdge {
            source_id,
            target_id,
            ..
        }
        | Method::RemoveEdge {
            source_id,
            target_id,
        } => CdcPre::Edge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            before: core
                .get_edge_properties(source_id, target_id)
                .into_iter()
                .next(),
        },
        _ => CdcPre::Skip,
    }
}

/// Emit the CDC event for a successfully-applied durable mutation, reading the
/// post-image from `core`. No-op for `CdcPre::Skip`.
pub fn emit_for_method(hub: &CdcHub, core: &GraphCore, graph: &str, method: &Method, pre: CdcPre) {
    match (method, pre) {
        (Method::AddNode { node_id, .. }, CdcPre::Node { before, .. }) => {
            let after = core.get_node_properties(node_id);
            let kind = if before.is_some() {
                CdcKind::UpdateNode
            } else {
                CdcKind::AddNode
            };
            hub.emit(graph, kind, node_id.clone(), String::new(), before, after);
        }
        (Method::CompareAndSetNodeFields { node_id, .. }, CdcPre::Node { before, .. }) => {
            // A CAS that didn't match leaves the blob unchanged; emit UpdateNode with
            // the post-image so an unchanged after == before is observable but harmless.
            let after = core.get_node_properties(node_id);
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                node_id.clone(),
                String::new(),
                before,
                after,
            );
        }
        (Method::RemoveNode { node_id }, CdcPre::Node { before, .. }) => {
            hub.emit(
                graph,
                CdcKind::RemoveNode,
                node_id.clone(),
                String::new(),
                before,
                None,
            );
        }
        (
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            },
            CdcPre::Edge { before, .. },
        ) => {
            hub.emit(
                graph,
                CdcKind::AddEdge,
                source_id.clone(),
                target_id.clone(),
                before,
                Some(properties_msgpack.clone()),
            );
        }
        (
            Method::RemoveEdge {
                source_id,
                target_id,
            },
            CdcPre::Edge { before, .. },
        ) => {
            hub.emit(
                graph,
                CdcKind::RemoveEdge,
                source_id.clone(),
                target_id.clone(),
                before,
                None,
            );
        }
        // A whole-graph wipe resets the change feed: the per-node changes are moot once
        // the graph is empty, so the feed rewinds to seq 0 and a consumer re-seeds.
        (Method::ClearGraph, _) => hub.reset_graph(graph),
        _ => {}
    }
}

/// Map a CDC kind to the trigger `op` string.
fn kind_op(kind: &CdcKind) -> &'static str {
    match kind {
        CdcKind::AddNode | CdcKind::AddEdge => "add",
        CdcKind::RemoveNode | CdcKind::RemoveEdge => "remove",
        CdcKind::UpdateNode => "update",
    }
}

/// Best-effort label extraction from a node/edge property msgpack blob: the first of
/// `type` / `node_type` / `label` present as a string (the same keys the label index
/// uses). Empty when the blob is absent / not an object / carries none of them.
fn extract_label(blob: Option<&[u8]>) -> String {
    let Some(bytes) = blob else {
        return String::new();
    };
    let Ok(val) = rmp_serde::from_slice::<serde_json::Value>(bytes) else {
        return String::new();
    };
    let Some(obj) = val.as_object() else {
        return String::new();
    };
    for key in ["type", "node_type", "label"] {
        if let Some(serde_json::Value::String(s)) = obj.get(key) {
            return s.clone();
        }
    }
    String::new()
}

/// Decode a numeric node-property field from a msgpack blob (0.0 if absent/non-numeric).
fn field_num(blob: &[u8], field: &str) -> f64 {
    rmp_serde::from_slice::<serde_json::Value>(blob)
        .ok()
        .and_then(|v| v.get(field).and_then(|f| f.as_f64()))
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    #[test]
    fn cdc_read_from_cursor_is_ordered_and_skips_seen() {
        let hub = CdcHub::new();
        let s0 = hub.emit(
            "g",
            CdcKind::AddNode,
            "n1".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        let s1 = hub.emit(
            "g",
            CdcKind::AddNode,
            "n2".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        assert_eq!((s0, s1), (0, 1));

        // Read from the start returns both in order.
        let all = hub.read("g", 0, 0).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].seq, 0);
        assert_eq!(all[0].node_id, "n1");
        assert_eq!(all[1].seq, 1);
        assert_eq!(all[1].label, "Doc");

        // Re-read from a later cursor skips the seen one.
        let next = hub.read("g", all.last().unwrap().seq + 1, 0).unwrap();
        assert!(next.is_empty());

        // A fresh write then a read from that cursor returns only it.
        hub.emit(
            "g",
            CdcKind::RemoveNode,
            "n1".into(),
            String::new(),
            None,
            None,
        );
        let tail = hub.read("g", 2, 0).unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].kind, CdcKind::RemoveNode);
    }

    #[test]
    fn continuous_count_query_matches_full_rerun() {
        let hub = CdcHub::new();
        hub.register_query(
            "cq".into(),
            ContinuousQuerySpec {
                graph: "g".into(),
                label: "Doc".into(),
                agg: ContinuousAgg::Count,
            },
            0.0,
        );
        // 3 Doc adds, 1 other-label add, 1 Doc remove → live Doc count = 2.
        for n in ["a", "b", "c"] {
            hub.emit(
                "g",
                CdcKind::AddNode,
                n.into(),
                String::new(),
                None,
                Some(props(serde_json::json!({"type": "Doc"}))),
            );
        }
        hub.emit(
            "g",
            CdcKind::AddNode,
            "x".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Other"}))),
        );
        hub.emit(
            "g",
            CdcKind::RemoveNode,
            "a".into(),
            String::new(),
            Some(props(serde_json::json!({"type": "Doc"}))),
            None,
        );
        let r = hub.read_query("cq").unwrap();
        assert_eq!(
            r.value, 2.0,
            "incremental count must equal the full re-run (2 live Docs)"
        );
    }

    #[test]
    fn continuous_sum_query_handles_update_delta() {
        let hub = CdcHub::new();
        hub.register_query(
            "sum".into(),
            ContinuousQuerySpec {
                graph: "g".into(),
                label: String::new(),
                agg: ContinuousAgg::Sum {
                    field: "amt".into(),
                },
            },
            0.0,
        );
        hub.emit(
            "g",
            CdcKind::AddNode,
            "n".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"amt": 10.0}))),
        );
        // Update n: 10 -> 25 (delta +15). before/after carried.
        hub.emit(
            "g",
            CdcKind::UpdateNode,
            "n".into(),
            String::new(),
            Some(props(serde_json::json!({"amt": 10.0}))),
            Some(props(serde_json::json!({"amt": 25.0}))),
        );
        assert_eq!(hub.read_query("sum").unwrap().value, 25.0);
    }

    #[test]
    fn trigger_fires_and_records_action() {
        let hub = CdcHub::new();
        hub.register_trigger(
            "t1".into(),
            "g".into(),
            "Alert".into(),
            "add".into(),
            props(serde_json::json!({"topic": "alerts"})),
        );
        // Non-matching label → no fire.
        hub.emit(
            "g",
            CdcKind::AddNode,
            "n0".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        assert!(hub.fired("g", 0, 0).is_empty());
        // Matching label + op → fires.
        hub.emit(
            "g",
            CdcKind::AddNode,
            "n1".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Alert"}))),
        );
        let fired = hub.fired("g", 0, 0);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].trigger, "t1");
        assert_eq!(fired[0].node_id, "n1");
        let action: serde_json::Value = rmp_serde::from_slice(&fired[0].action).unwrap();
        assert_eq!(action["topic"], "alerts");
    }

    #[test]
    fn reset_graph_rewinds_feed_and_continuous_query() {
        let hub = CdcHub::new();
        hub.register_query(
            "cnt".into(),
            ContinuousQuerySpec {
                graph: "g".into(),
                label: String::new(),
                agg: ContinuousAgg::Count,
            },
            0.0,
        );
        hub.emit(
            "g",
            CdcKind::AddNode,
            "a".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        hub.emit(
            "g",
            CdcKind::AddNode,
            "b".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        assert_eq!(hub.head_seq("g"), 2);
        assert_eq!(hub.read_query("cnt").unwrap().value, 2.0);

        // ClearGraph resets the feed: seq rewinds to 0, ring empties, CQ resets.
        hub.reset_graph("g");
        assert_eq!(hub.head_seq("g"), 0);
        assert!(hub.read("g", 0, 0).unwrap().is_empty());
        assert_eq!(hub.read_query("cnt").unwrap().value, 0.0);

        // A post-reset write is seq 0 again (a consumer re-seeds from 0).
        let seq = hub.emit(
            "g",
            CdcKind::AddNode,
            "c".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        assert_eq!(seq, 0);
        assert_eq!(hub.read_query("cnt").unwrap().value, 1.0);
    }
}
