//! Change-Data-Capture hub + continuous queries + subscriptions/triggers
//! (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230, feature `streaming`).
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
#[cfg(any(feature = "owl", feature = "cdc-kafka"))]
use std::sync::OnceLock;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::wire::{
    CdcEvent, CdcKind, CdcReadResult, ContinuousAgg, ContinuousQueryResult, ContinuousQuerySpec,
    FiredAction, FiredTriggersResult, TriggerInfo, WatchBatch,
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
    /// Next seq to assign (== count of events ever emitted for this graph, in
    /// THIS epoch — see `CdcHub::epoch`). Doubles as the change-ring's `head_seq`.
    next_seq: u64,
    /// Bounded ring of recent events (front = oldest retained).
    ring: VecDeque<CdcEvent>,
    /// Seq of the oldest event still retained (`ring.front().seq`); a `from_seq`
    /// below this is behind the window (B-8: the change ring's `watermark`).
    oldest_seq: u64,
    /// Fired-trigger log (CONCEPT:EG-KG.query.wire-codec), its own monotonic cursor.
    /// Doubles as the fired log's `head_seq` (B-8 follow-up).
    next_fire: u64,
    fired: VecDeque<FiredAction>,
    /// `fire_seq` of the oldest retained firing (`fired.front().fire_seq`); a
    /// `from_seq` below this is behind the fired log's window (B-8 follow-up: the
    /// fired log's own `watermark`, mirroring `oldest_seq` for the change ring).
    fired_oldest: u64,
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
            fired_oldest: 0,
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

/// External sink hook installed at startup by CA-11's Kafka bridge (feature
/// `cdc-kafka`). A tiny in-crate trait — NOT `eg_stream::sink::CdcSink`
/// directly — so `cdc.rs` (read by every `streaming` consumer, not just
/// `cdc-kafka` builds) takes on no new dependency; `crate::server::cdc_sink`
/// owns the real Kafka producer and the DEC-CA-03 envelope shape, and
/// adapts itself onto this trait before calling [`CdcHub::install_sink`].
#[cfg(feature = "cdc-kafka")]
pub trait ExternalCdcSink: Send + Sync {
    /// Forward one ordered change. MUST NOT block (`CdcHub::emit` runs on
    /// the durable-commit path) — an implementation queues/drops, never
    /// waits on the network. Best-effort: a failure here never fails the
    /// commit that produced `event` (`DEC-CA-01`/`DEC-CA-03`: "Kafka is
    /// transport only").
    fn emit(&self, event: &CdcEvent);

    /// Block up to `timeout_ms` for every queued event to reach the broker.
    /// For graceful shutdown / tests, never the commit path. Default no-op.
    fn flush(&self, _timeout_ms: u64) {}
}

/// The CDC hub on `ServerState`. One per engine; routes by graph name.
pub struct CdcHub {
    feeds: Mutex<HashMap<String, GraphFeed>>,
    queries: Mutex<HashMap<String, ContinuousQuery>>,
    triggers: Mutex<HashMap<String, Trigger>>,
    cap: usize,
    /// Process-lifetime identity of THIS hub instance (B-8, 2026-08-13): a fresh,
    /// random id minted once here and stable for the hub's life. `CdcHub` is a
    /// purely in-memory ring (module doc above) with no cross-restart continuity —
    /// a process restart constructs a brand-new `CdcHub` (`main.rs`), hence a brand
    /// new `epoch`, and every graph's seq numbering restarts from 0 in it. A
    /// `from_seq` cursor issued against a PRIOR epoch can be numerically "in range"
    /// of the new epoch's `[watermark, head_seq]` window without naming the same
    /// events at all — a purely numeric bounds check cannot tell the epochs apart.
    /// Every `CdcRead`/`Watch`/`FiredTriggers` response carries this epoch so a
    /// caller that persists `(epoch, from_seq)` alongside its cursor can detect a
    /// restart as an explicit, provable condition instead of inferring completeness
    /// from a merely-empty or merely-short read. See `CdcReadResult`'s doc.
    epoch: u64,
    /// Live CEP standing-query surface (CONCEPT:EG-KG.query.protocol-types), feature `stream`. Created LAZILY
    /// on the first `CepSubscribe` (`cep_surface`), so a CDC feed with no CEP subscriber
    /// pays nothing and the drain-task spawn always happens inside the async handler (a
    /// runtime is live). Once set, `emit` feeds every change into it. A `OnceLock` so the
    /// hot emit path reads it lock-free.
    #[cfg(feature = "stream")]
    cep: std::sync::OnceLock<Arc<crate::server::cep::CepSurface>>,
    /// The opt-in reasoning cascade (W3.6/E16, `REASON_ON_WRITE`), installed
    /// once at startup ONLY when armed (`install_reasoning_cascade`). `None`
    /// (the default for every existing deployment/test) means `emit` pays
    /// exactly one `OnceLock::get()` — a single atomic load — before
    /// returning: the cost gate the opt-in exists to enforce. Same lazy,
    /// set-once-from-outside idiom as `cep` above.
    #[cfg(feature = "owl")]
    reasoning_cascade: OnceLock<Arc<crate::server::reasoning_cascade::ReasoningCascade>>,
    /// CA-11's external Kafka sink (feature `cdc-kafka`), installed once at
    /// startup by `cdc_sink::install_from_env` ONLY when
    /// `EPISTEMIC_GRAPH_CDC_KAFKA_BROKERS` is set. `None` (every build
    /// without `cdc-kafka`, and every `cdc-kafka` build with the env var
    /// unset) means `emit` pays one `OnceLock::get()` and nothing else —
    /// same cost-gate idiom as `cep`/`reasoning_cascade` above, and the
    /// "feature off ⇒ zero behavior change" contract the lane brief
    /// requires.
    #[cfg(feature = "cdc-kafka")]
    sink: OnceLock<Arc<dyn ExternalCdcSink>>,
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
            // B-8: minted fresh per hub (== per process, in production). A 64-bit
            // random draw is astronomically unlikely to collide across restarts,
            // and the cost of a missed collision is a rare false-negative gap
            // detection (identical failure mode the pre-fix code always had), never
            // a false positive that would wrongly reject a legitimate resume.
            epoch: rand::random(),
            #[cfg(feature = "stream")]
            cep: std::sync::OnceLock::new(),
            #[cfg(feature = "owl")]
            reasoning_cascade: OnceLock::new(),
            #[cfg(feature = "cdc-kafka")]
            sink: OnceLock::new(),
        }
    }

    /// Install the external Kafka sink (CA-11). Called at most once, at
    /// startup, from `cdc_sink::install_from_env`. A second call is a
    /// silent no-op — same idiom as `install_reasoning_cascade` below.
    #[cfg(feature = "cdc-kafka")]
    pub fn install_sink(&self, sink: Arc<dyn ExternalCdcSink>) {
        let _ = self.sink.set(sink);
    }

    /// Flush the installed sink (if any), blocking up to `timeout_ms` for
    /// delivery. For graceful shutdown / delivery-proving tests, NOT the
    /// commit path — `emit` never calls this.
    #[cfg(feature = "cdc-kafka")]
    pub fn flush_sink(&self, timeout_ms: u64) {
        if let Some(sink) = self.sink.get() {
            sink.flush(timeout_ms);
        }
    }

    /// The live CEP standing-query surface (CONCEPT:EG-KG.query.protocol-types), lazily created on first use.
    /// Its drain task is spawned here — always called from the async `CepSubscribe` handler,
    /// so a Tokio runtime is live. Subsequent `emit`s feed the surface via [`CdcHub::emit`].
    #[cfg(feature = "stream")]
    pub fn cep_surface(&self) -> Arc<crate::server::cep::CepSurface> {
        self.cep
            .get_or_init(crate::server::cep::CepSurface::new)
            .clone()
    }

    /// Install the reasoning cascade (W3.6/E16). Called at most once, at
    /// startup, ONLY when `REASON_ON_WRITE` is armed
    /// (`ReasoningCascade::is_active`) — see `main.rs`. A second call is a
    /// silent no-op (the cascade is fixed for this hub's lifetime, same as
    /// `cep` above); every test/deployment that never calls this leaves
    /// `emit`'s hook a single cheap `OnceLock::get()` miss.
    #[cfg(feature = "owl")]
    pub fn install_reasoning_cascade(
        &self,
        cascade: Arc<crate::server::reasoning_cascade::ReasoningCascade>,
    ) {
        let _ = self.reasoning_cascade.set(cascade);
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

    /// Emit one change into `graph`'s feed (CONCEPT:EG-KG.query.streaming-cdc-subscriptions). Assigns the per-graph
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
        // Bound outside the feeds-lock block so the post-lock matview hooks can read the
        // event without re-borrowing the feed (matview-only; a no-op field otherwise).
        #[cfg(feature = "matview")]
        let emitted_event;
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
            // Feed the live CEP standing-query engine (CONCEPT:EG-KG.query.protocol-types) if a subscriber has
            // created the surface. Lock-free check; the map→publish is a bounded broadcast
            // send that never blocks the writer (a full bus drops the oldest event).
            #[cfg(feature = "stream")]
            if let Some(cep) = self.cep.get() {
                cep.feed_change(&event);
            }
            // CA-11: forward to the external Kafka sink if one is installed
            // (`DEC-CA-03`'s `eg.cdc.<graph>`). Lock-free check when unset
            // (every build without `cdc-kafka`, and every `cdc-kafka` build
            // with no broker configured); `KafkaCdcSink::emit` itself is
            // non-blocking (queues into librdkafka's local buffer, never
            // waits on the network), so this stays inside the feeds lock on
            // the same footing as `maintain`/`fire_triggers`/`cep` above.
            #[cfg(feature = "cdc-kafka")]
            if let Some(sink) = self.sink.get() {
                sink.emit(&event);
            }
            let notify = feed.notify.clone();
            #[cfg(feature = "matview")]
            {
                emitted_event = event;
            }
            (seq, notify)
        };
        // CDC INVALIDATION for plan-backed matviews (CONCEPT:EG-KG.storage.matview-cdc-invalidation):
        // a committed change to `graph` retires every plan-backed matview over it, so the
        // next `Get` recomputes. Taken AFTER the feeds lock is dropped (the manager owns a
        // separate lock — no nested lock ordering). The graph's OCC version already bumped
        // on the write (the (query_hash, version) result-cache key retires the cached
        // bytes); this is the belt-and-braces manager-side signal reusing that discipline.
        #[cfg(feature = "matview")]
        {
            // Recompute-mode views: blunt stale flag (belt-and-braces on the OCC version).
            crate::server::matview::note_change(graph);
            // Incremental-mode views: DBSP delta maintenance (CONCEPT:EG-KG.storage.incremental-matview)
            // — fold this exact before/after image through the compiled circuit so the
            // view's `current` stays fresh WITHOUT a full recompute. One is a no-op per
            // view depending on its mode.
            crate::server::matview::apply_delta(graph, &emitted_event);
        }
        // Reasoning auto-cascade (W3.6/E16, CONCEPT: opt-in `REASON_ON_WRITE`): note
        // the write for the debounced OWL/RL closure refresh. THE COST GATE lives
        // here: with no cascade installed (every deployment that never armed
        // `REASON_ON_WRITE`, and every existing test), this is ONE `OnceLock::get()`
        // — a single atomic load — and nothing else. With a cascade installed but
        // `graph` not in its opted-in set, `note_write` itself is one hashset lookup.
        // Actual reasoning work NEVER happens on this path — only the periodic sweep
        // task (`reasoning_cascade::spawn`) runs `add_axioms`/`classify`.
        #[cfg(feature = "owl")]
        if let Some(cascade) = self.reasoning_cascade.get() {
            cascade.note_write(graph);
        }
        notify.notify_waiters();
        seq
    }

    /// This hub instance's process-lifetime epoch id (B-8). See the field's doc.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Read the ordered feed for `graph` from `from_seq` (inclusive), capped at
    /// `limit` (0 ⇒ 1024) (B-8, 2026-08-13). `gap: true` when `from_seq` fell behind
    /// the retained ring window in THIS epoch — `events` is then always empty and
    /// the caller must re-seed rather than treat the empty result as "caught up".
    /// `gap: false` with an empty `events` genuinely means "caught up" (cursor
    /// at/after the head). Every result carries `watermark`/`head_seq`/`epoch` so a
    /// caller resuming a PERSISTED cursor across a restart (a fresh epoch, where the
    /// numeric bounds check below cannot itself detect the discontinuity — see
    /// `CdcReadResult`'s doc) can still prove it by comparing `epoch` to what it
    /// last observed.
    pub fn read(&self, graph: &str, from_seq: u64, limit: u32) -> CdcReadResult {
        let cap = if limit == 0 { 1024 } else { limit as usize };
        let feeds = self.feeds.lock();
        let Some(feed) = feeds.get(graph) else {
            // No feed yet for this graph in this epoch. `from_seq == 0` is a
            // legitimate cold start (nothing to read yet — not a gap). Any other
            // `from_seq` claims history this epoch has never produced, which is
            // exactly the epoch-reset shape the checks below cover once a feed
            // exists, so it gets the same explicit signal here.
            return if from_seq == 0 {
                CdcReadResult::ok(Vec::new(), 0, 0, self.epoch)
            } else {
                CdcReadResult::gap(0, 0, self.epoch)
            };
        };
        // Two distinct, both-explicit gap conditions (B-8):
        //   - `from_seq < oldest_seq`: within-epoch trim — this epoch's own ring
        //     dropped entries the cursor still needs.
        //   - `from_seq > next_seq`: the cursor claims to have already seen events
        //     this epoch has never produced — the numerically-detectable slice of
        //     the epoch-reset case (a restart with FEWER events emitted so far than
        //     the prior epoch's cursor implies). The remaining slice (a restart
        //     that happens to have produced enough fresh events to make the old
        //     cursor look numerically plausible again) is NOT detectable from
        //     numbers alone — that's what `epoch` on the returned result is for.
        if from_seq < feed.oldest_seq || from_seq > feed.next_seq {
            return CdcReadResult::gap(feed.oldest_seq, feed.next_seq, self.epoch);
        }
        let events = feed
            .ring
            .iter()
            .filter(|e| e.seq >= from_seq)
            .take(cap)
            .cloned()
            .collect();
        CdcReadResult::ok(events, feed.oldest_seq, feed.next_seq, self.epoch)
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

    /// Reset a graph's change feed (CONCEPT:EG-KG.query.streaming-cdc-subscriptions): drop its retained ring + fired
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
                feed.fired_oldest = 0;
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
    /// `Notify`-await loop, calling this each wake. B-8: `Watch` tails the identical
    /// ring `read` does, so it carries the identical `gap`/`watermark`/`head_seq`/
    /// `epoch` signalling — see `read`'s doc.
    pub fn watch_batch(&self, graph: &str, from_seq: u64, label: &str, limit: u32) -> WatchBatch {
        let result = self.read(graph, from_seq, limit);
        if result.gap {
            return WatchBatch {
                events: Vec::new(),
                next_seq: from_seq,
                gap: true,
                watermark: result.watermark,
                head_seq: result.head_seq,
                epoch: result.epoch,
            };
        }
        let events: Vec<CdcEvent> = result
            .events
            .into_iter()
            .filter(|e| label.is_empty() || e.label == label)
            .collect();
        let next_seq = events
            .last()
            .map(|e| e.seq + 1)
            .unwrap_or_else(|| from_seq.max(result.head_seq));
        WatchBatch {
            events,
            next_seq,
            gap: false,
            watermark: result.watermark,
            head_seq: result.head_seq,
            epoch: result.epoch,
        }
    }

    // ── Continuous queries (CONCEPT:EG-KG.query.streaming-cdc-subscriptions) ────────────────────────────────

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

    /// Snapshot the immutable graph/filter specification so the served handler
    /// can re-evaluate the aggregate against its caller-specific RLS projection.
    pub fn query_spec(&self, name: &str) -> Option<ContinuousQuerySpec> {
        self.queries
            .lock()
            .get(name)
            .map(|query| query.spec.clone())
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

    // ── Triggers / reactions (CONCEPT:EG-KG.query.wire-codec) ──────────────────────────────

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

    /// Poll the fired-trigger log for `graph` from `from_seq` (inclusive) (B-8
    /// follow-up, 2026-08-13). Same gap signalling as `read`, scoped to the
    /// fired-trigger log's own `fire_seq` cursor and its own `fired_oldest`/
    /// `next_fire` watermark/head pair — see `read`'s doc and
    /// `FiredTriggersResult`'s doc.
    pub fn fired(&self, graph: &str, from_seq: u64, limit: u32) -> FiredTriggersResult {
        let cap = if limit == 0 { 1024 } else { limit as usize };
        let feeds = self.feeds.lock();
        let Some(feed) = feeds.get(graph) else {
            return if from_seq == 0 {
                FiredTriggersResult::ok(Vec::new(), 0, 0, self.epoch)
            } else {
                FiredTriggersResult::gap(0, 0, self.epoch)
            };
        };
        if from_seq < feed.fired_oldest || from_seq > feed.next_fire {
            return FiredTriggersResult::gap(feed.fired_oldest, feed.next_fire, self.epoch);
        }
        let fired = feed
            .fired
            .iter()
            .filter(|f| f.fire_seq >= from_seq)
            .take(cap)
            .cloned()
            .collect();
        FiredTriggersResult::ok(fired, feed.fired_oldest, feed.next_fire, self.epoch)
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
            // Bound the fired log to the same cap as the ring, maintaining
            // `fired_oldest` (B-8 follow-up) the same way `emit` maintains
            // `oldest_seq` for the change ring.
            if feed.fired.len() > feed.cap {
                feed.fired.pop_front();
                feed.fired_oldest = feed.fired.front().map(|f| f.fire_seq).unwrap_or(fire_seq);
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
        | Method::CreateNodeIfAbsent { node_id, .. }
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
        (Method::CreateNodeIfAbsent { node_id, .. }, CdcPre::Node { before: None, .. }) => {
            hub.emit(
                graph,
                CdcKind::AddNode,
                node_id.clone(),
                String::new(),
                None,
                core.get_node_properties(node_id),
            );
        }
        (
            Method::CreateNodeIfAbsent { .. },
            CdcPre::Node {
                before: Some(_), ..
            },
        ) => {
            // A losing create is a durable false result, not a row update.
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
        // FromMsgpack/Reconcile both replace the graph's entire node/edge content
        // with an imported or merged authoritative image (`core.from_msgpack`) --
        // the same "whole graph replaced" shape as `ClearGraph`, so any prior
        // incremental deltas are equally moot. W1c: previously fell to the `_`
        // catch-all (no CDC at all) despite being durable + GATEWAY_ROUTED.
        (Method::FromMsgpack { .. }, _) => hub.reset_graph(graph),
        (Method::Reconcile { .. }, _) => hub.reset_graph(graph),
        // ── W1c: close the 9-method audit/CDC-visibility gap for the remaining
        // durable admin/ledger methods. None of these map to a single node/edge
        // row, so -- consistent with `emit_served_modality`'s reserved-marker-id
        // shape below -- each emits ONE `UpdateNode` marker event with a
        // reserved `__`-prefixed id (never a real node id) and no before/after
        // payload, giving CDC consumers an observable "this happened" signal
        // without fabricating a fake property diff. ──
        (Method::ApplyMutation { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__apply_mutation".to_string(),
                String::new(),
                None,
                None,
            );
        }
        (Method::ApplyMultisigMutation { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__apply_multisig_mutation".to_string(),
                String::new(),
                None,
                None,
            );
        }
        // W2.5 fleet server registry: defense-in-depth marker mirroring
        // `ApplyMultisigMutation` above -- this variant self-translates into
        // `Method::AddNode` in `dispatch.rs` BEFORE ever reaching `commit_mutation`, so
        // the REAL CDC event for a registration is AddNode's own (`srv:<name>`,
        // AddNode/UpdateNode kind), not this marker. Unreachable via the single-node
        // delegation path today.
        (Method::RegisterServer { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__register_server".to_string(),
                String::new(),
                None,
                None,
            );
        }
        #[cfg(feature = "shacl")]
        (Method::IcvConfigure { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__icv_configure".to_string(),
                String::new(),
                None,
                None,
            );
        }
        #[cfg(feature = "reasoning")]
        (Method::RunDatalogReasoning { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__run_datalog_reasoning".to_string(),
                String::new(),
                None,
                None,
            );
        }
        (Method::ClearLedger, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__ledger".to_string(),
                String::new(),
                None,
                None,
            );
        }
        (Method::ApplyLedger { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__ledger".to_string(),
                String::new(),
                None,
                None,
            );
        }
        (Method::CompactNodesByType { .. }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                "__compact_nodes_by_type".to_string(),
                String::new(),
                None,
                None,
            );
        }
        (Method::ApplyChangeEnvelope { envelope }, _) => {
            hub.emit(
                graph,
                CdcKind::UpdateNode,
                envelope.content_version.object_id.clone(),
                String::new(),
                None,
                None,
            );
        }
        // The batch coordinator emits one change event per envelope (mirroring the
        // single method) so policy `emits_cdc: true` stays consistent; at runtime the
        // per-envelope durable outbox is the authoritative change feed.
        (Method::ApplyChangeEnvelopes { envelopes }, _) => {
            for envelope in envelopes {
                hub.emit(
                    graph,
                    CdcKind::UpdateNode,
                    envelope.content_version.object_id.clone(),
                    String::new(),
                    None,
                    None,
                );
            }
        }
        #[cfg(feature = "modality-serving")]
        (Method::ServedModality { op }, _) if op.mutates() => {
            use eg_types::ServedModalityOp;
            let modality = match op {
                ServedModalityOp::Ingest { modality, .. }
                | ServedModalityOp::IngestStream { modality, .. }
                | ServedModalityOp::Delete { modality, .. }
                | ServedModalityOp::MoveToCold { modality, .. }
                | ServedModalityOp::Restore { modality, .. } => modality,
                ServedModalityOp::CollectTombstones { modality, .. } => modality,
                _ => return,
            };
            emit_served_modality(hub, graph, *modality);
        }
        _ => {}
    }
}

/// Emit the privacy-safe category marker used by both the single-node mutation
/// gateway and the sanitized Raft state machine. No occurrence or partition id is
/// exposed to CDC consumers.
#[cfg(feature = "modality-serving")]
pub(crate) fn emit_served_modality(
    hub: &CdcHub,
    graph: &str,
    modality: eg_types::ServedModalityKind,
) {
    let node_id = match modality {
        eg_types::ServedModalityKind::Document => "__served_modality_document",
        eg_types::ServedModalityKind::Image => "__served_modality_image",
        eg_types::ServedModalityKind::Audio => "__served_modality_audio",
        eg_types::ServedModalityKind::Video => "__served_modality_video",
    };
    hub.emit(
        graph,
        CdcKind::UpdateNode,
        node_id.to_string(),
        String::new(),
        None,
        None,
    );
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
    let Ok(val) = eg_types::msgpack::decode_property_value(bytes) else {
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
    eg_types::msgpack::decode_property_value(blob)
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

    /// Wiring proof (W3.6/E16): `emit` on the REAL hook only notes a write for a
    /// graph actually in the installed cascade's opted-in set; every other graph
    /// leaves ZERO cascade bookkeeping, through the exact same `CdcHub::emit` path
    /// production traffic takes (not a direct call into `ReasoningCascade`).
    #[cfg(feature = "owl")]
    #[test]
    fn emit_notes_the_installed_cascade_only_for_its_opted_in_graph() {
        use crate::server::reasoning_cascade::ReasoningCascade;
        use std::time::Duration;

        let hub = CdcHub::new();
        let cascade = std::sync::Arc::new(ReasoningCascade::new(
            ["g-in".to_string()].into_iter().collect(),
            Duration::from_secs(60),
        ));
        hub.install_reasoning_cascade(cascade.clone());

        hub.emit(
            "g-in",
            CdcKind::AddNode,
            "n1".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        hub.emit(
            "g-out",
            CdcKind::AddNode,
            "n2".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );

        assert!(
            !cascade.has_no_state("g-in"),
            "the opted-in graph's write must be noted via the real emit() hook"
        );
        assert!(
            cascade.has_no_state("g-out"),
            "a non-opted-in graph's write must leave NO cascade state, even through \
             the real emit() hook"
        );
    }

    /// A hub with NO cascade installed (every existing deployment/test) never
    /// touches `reasoning_cascade` at all — `emit`'s hook is a single
    /// `OnceLock::get()` miss. This is implicitly exercised by every OTHER test
    /// in this module (none of them install a cascade); this test just pins the
    /// unconfigured path explicitly.
    #[cfg(feature = "owl")]
    #[test]
    fn emit_with_no_cascade_installed_behaves_exactly_as_before() {
        let hub = CdcHub::new();
        let seq = hub.emit(
            "g",
            CdcKind::AddNode,
            "n1".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        assert_eq!(seq, 0);
        assert_eq!(hub.read("g", 0, 0).events.len(), 1);
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
        let result = hub.read("g", 0, 0);
        assert!(!result.gap);
        let all = result.events;
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].seq, 0);
        assert_eq!(all[0].node_id, "n1");
        assert_eq!(all[1].seq, 1);
        assert_eq!(all[1].label, "Doc");

        // Re-read from a later cursor (exactly caught up: from_seq == head_seq) is
        // NOT a gap and returns empty.
        let next = hub.read("g", all.last().unwrap().seq + 1, 0);
        assert!(!next.gap);
        assert!(next.events.is_empty());

        // A fresh write then a read from that cursor returns only it.
        hub.emit(
            "g",
            CdcKind::RemoveNode,
            "n1".into(),
            String::new(),
            None,
            None,
        );
        let tail = hub.read("g", 2, 0);
        assert!(!tail.gap);
        assert_eq!(tail.events.len(), 1);
        assert_eq!(tail.events[0].kind, CdcKind::RemoveNode);
    }

    /// B-8 (2026-08-13): a cursor beyond the current epoch's head is an explicit
    /// `gap`, not a silently-empty "caught up" read — this is the numerically-
    /// detectable slice of the epoch-reset failure mode `CdcReadResult` documents
    /// (e.g. a consumer resuming a PERSISTED cursor after the process restarted and
    /// produced fewer events so far than the old cursor implies). Proves the fix
    /// directly: before this change, `read`'s only check was `from_seq <
    /// oldest_seq`, so `from_seq > next_seq` was served as an empty `Ok(vec![])`
    /// indistinguishable from a genuine "nothing new yet".
    #[test]
    fn cdc_read_from_seq_ahead_of_head_is_an_explicit_gap_not_a_silent_empty_read() {
        let hub = CdcHub::new();
        hub.emit(
            "g",
            CdcKind::AddNode,
            "n1".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Doc"}))),
        );
        // Exactly one event exists (head_seq == 1). A cursor of 5 claims to have
        // already seen events this feed never produced.
        let result = hub.read("g", 5, 0);
        assert!(
            result.gap,
            "from_seq(5) > head_seq(1) must be flagged as a gap, not served as caught-up"
        );
        assert!(result.events.is_empty());
        assert_eq!(result.head_seq, 1);
        assert_eq!(result.watermark, 0);
    }

    /// B-8: `CdcRead`'s `epoch` lets a caller prove a restart even in the ONE case
    /// the numeric `from_seq <= head_seq` bounds check cannot itself catch — a
    /// restarted hub that has already produced enough fresh events to make the old
    /// cursor look numerically plausible again in the NEW epoch. Two independently
    /// constructed hubs stand in for "the same process across a restart": the
    /// second hub's `epoch` differs from the first's, even though, numerically,
    /// the old cursor is perfectly "in range" of the new hub's ring.
    #[test]
    fn cdc_epoch_differs_across_a_simulated_restart_even_when_from_seq_is_numerically_in_range() {
        let hub_before = CdcHub::new();
        for n in ["a", "b", "c"] {
            hub_before.emit(
                "g",
                CdcKind::AddNode,
                n.into(),
                String::new(),
                None,
                Some(props(serde_json::json!({"type": "Doc"}))),
            );
        }
        let cursor = hub_before.head_seq("g"); // 3
        let epoch_before = hub_before.read("g", 0, 0).epoch;

        // Simulate a process restart: a brand-new hub, then enough fresh writes to
        // make `cursor` (3) numerically valid again.
        let hub_after = CdcHub::new();
        for n in ["x", "y", "z", "w", "v"] {
            hub_after.emit(
                "g",
                CdcKind::AddNode,
                n.into(),
                String::new(),
                None,
                Some(props(serde_json::json!({"type": "Doc"}))),
            );
        }
        let result = hub_after.read("g", cursor, 0);
        // The numeric check alone cannot see a problem: cursor(3) is within
        // [watermark(0), head_seq(5)] of the new epoch, so `gap` is false and a
        // real (but WRONG) batch of events is returned.
        assert!(!result.gap);
        assert_ne!(
            epoch_before, result.epoch,
            "the epoch must differ across a restart even though from_seq stayed \
             numerically in range -- this is the signal a caller compares to catch \
             the case `gap` structurally cannot"
        );
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
        assert!(!hub.fired("g", 0, 0).gap);
        assert!(hub.fired("g", 0, 0).fired.is_empty());
        // Matching label + op → fires.
        hub.emit(
            "g",
            CdcKind::AddNode,
            "n1".into(),
            String::new(),
            None,
            Some(props(serde_json::json!({"type": "Alert"}))),
        );
        let result = hub.fired("g", 0, 0);
        assert!(!result.gap);
        let fired = result.fired;
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
        let post_reset = hub.read("g", 0, 0);
        assert!(!post_reset.gap);
        assert!(post_reset.events.is_empty());
        assert_eq!(hub.read_query("cnt").unwrap().value, 0.0);

        // B-8: a consumer that still holds its PRE-reset cursor (2) gets an
        // explicit gap, not a silent "caught up" — `ClearGraph` is the in-process,
        // numerically-detectable instance of the same epoch-reset shape a process
        // restart produces (see `cdc_epoch_differs_across_a_simulated_restart_*`
        // for the case a bare bounds check cannot catch on its own).
        let stale_cursor_read = hub.read("g", 2, 0);
        assert!(
            stale_cursor_read.gap,
            "a cursor from before reset_graph must be flagged, not silently served empty"
        );

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
