//! EH-286: automatic causal-chain capture for raft cluster tests.
//!
//! A failing multi-node cluster test used to report only its assertion (e.g.
//! `"placement group 7 has no current leader"`) with no causal chain — roughly four
//! investigation cycles were spent before anyone enabled openraft's DEBUG tracing by
//! hand, which immediately revealed the mechanism
//! (`HeartbeatWorker ... failed to send a heartbeat: Err(Elapsed(()))`, one group
//! cycling 8 terms over ~50s). See
//! `plans/refactor/architecture/RAFT-CLUSTER-RELIABILITY-DESIGN.md` §4.
//!
//! ## Why a ring buffer, not "just turn tracing on"
//!
//! The design doc records that manually enabling DEBUG tracing did not just reveal
//! the mechanism — it also raised reproduction from 80% to 100%, because the added
//! latency (formatting + a synchronous, line-buffered write to stderr on every
//! heartbeat/replication event, on the exact hot path being timed) widened the
//! losing window. **A capture mechanism that changes the thing it measures is a bad
//! instrument.** So this module never performs I/O on the happy path: matched events
//! are appended to a bounded in-memory ring ([`Ring::push`], one `String` push behind
//! a mutex, no formatting beyond what `tracing`'s own field visitor already does) and
//! are only formatted and written once, on an actual test failure, via a
//! [`std::panic`] hook. The ring is bounded ([`CAPACITY`]) so a long-running or
//! looping test cannot grow it without limit.
//!
//! ## What gets captured
//!
//! Only two target prefixes, at `DEBUG` and above: `openraft` (replication,
//! heartbeat, election internals) and [`RAFT_MODULE_TARGET`] (this crate's own raft
//! module — group lifecycle, leader routing). Everything else stays filtered out at
//! the subscriber, so installing this globally does not turn on logging for the rest
//! of the test binary. `openraft` commonly attaches node/group identity to a SPAN
//! around its core loop rather than repeating it on every leaf event, so
//! [`CaptureLayer::on_new_span`] stashes each span's fields and
//! [`append_ancestor_span_fields`] folds the whole ancestor chain into every event a
//! span contains — otherwise most captured lines would be missing exactly the
//! node/group identity this exists to preserve.
//!
//! ## Wiring
//!
//! [`init`] is idempotent and installs (once per test binary): the capturing
//! subscriber as the process's global default `tracing` dispatcher, and a panic hook
//! that dumps [`render_dump`] to stderr after the normal panic message. `node::start`
//! is not the ONLY way a test builds a raft node in this crate, so `init()` is called
//! from every construction path this crate's own `raft::tests` module uses, found by
//! grepping it for `MultiRaft::` and `node::start`, not just one:
//! * `super::super::tests::cluster_cfg_with_groups` — every `node::start` call in
//!   `raft::tests` (directly, or via `cluster_cfg`, which forwards to it). Covers
//!   `placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster`, one of
//!   EH-286's two named examples.
//! * `super::super::tests::start_multi` — every DIRECT `MultiRaft::start` call in
//!   `raft::tests` that bypasses `node::start` entirely (membership/leader-rebalance
//!   tests that manage a `MultiRaft` handle by hand). Covers
//!   `multi_node_group_join_then_leader_rebalance`, EH-286's other named example.
//!
//! A single test reaching either of these reaches the same idempotent `init()`, so
//! this is the default across every way `raft::tests` starts a raft node or a bare
//! `MultiRaft`, not something a human must remember to opt into on a new one.
//!
//! **NOT covered:** `src/raft/harness/cluster.rs`'s own private `node::start` path
//! (`Cluster::start` and its post-kill restart, used by `gauntlet_test`/
//! `catchup_test` and five other `harness::cluster` consumers). That file is already
//! at its `statements_per_file` cap (257 of 250 — a pre-existing, tolerated
//! violation) before any change here; adding the one statement this wiring needs
//! measurably regresses an already-over-cap file, which this program's build
//! contract treats as forbidden regardless of the file already being over cap
//! (`grep`-verified: 257 statements at `HEAD`, 258 with the one-line
//! `super::trace_capture::init();` addition attempted and reverted). Splitting
//! `cluster.rs` to make room is a legitimate follow-up but a separate, larger
//! refactor outside this file's own layout, not a one-line addition; escalated
//! rather than forced through. Neither of EH-286's two named tests uses this path.
//!
//! ## Known limitation: one buffer, whole test binary
//!
//! The ring is process-global, not per-test. Rust's default test runner runs `#[test]`
//! functions concurrently, so a dump on one test's failure can include a few
//! interleaved lines from another cluster test running at the same moment. Every line
//! still carries its own sequence number, elapsed time, and thread id, so a human (or
//! EH-287's investigation) can still separate them; for a clean single-test capture,
//! run with `--test-threads=1`, which is how EH-288's own investigation was already
//! being conducted ("the quietest host available").

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tracing::field::{Field, Visit};
use tracing::{span, Event, Level, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

use crate::lock_recovery::LockRecovery;

/// Upper bound on retained events. Oldest evicted first — bounds memory for a long
/// or looping cluster test regardless of how much openraft chatter it produces.
const CAPACITY: usize = 4096;

/// This crate's own raft-module traces (group lifecycle, leader routing) — the other
/// half of the causal chain alongside openraft's internal replication/heartbeat
/// events.
const RAFT_MODULE_TARGET: &str = "epistemic_graph::raft";

/// One retained trace line.
struct CapturedEvent {
    seq: usize,
    elapsed: Duration,
    thread: String,
    level: Level,
    target: &'static str,
    message: String,
    fields: String,
}

/// The bounded event store plus the sequence/clock it stamps entries with.
struct Ring {
    start: Instant,
    seq: AtomicUsize,
    events: Mutex<VecDeque<CapturedEvent>>,
}

impl Ring {
    fn new() -> Self {
        Ring {
            start: Instant::now(),
            seq: AtomicUsize::new(0),
            events: Mutex::new(VecDeque::with_capacity(CAPACITY)),
        }
    }

    /// Append one event, evicting the oldest if already at [`CAPACITY`].
    fn push(
        &self,
        level: Level,
        target: &'static str,
        thread: String,
        message: String,
        fields: String,
    ) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let elapsed = self.start.elapsed();
        let mut events = self
            .events
            .lock_recovering("raft trace capture ring buffer");
        if events.len() >= CAPACITY {
            events.pop_front();
        }
        events.push_back(CapturedEvent {
            seq,
            elapsed,
            thread,
            level,
            target,
            message,
            fields,
        });
    }

    /// Render every retained event, ordered oldest-first, one line each:
    /// sequence number, elapsed time since the ring was created, thread, level,
    /// target, and the message with any node/group identity fields appended.
    fn render(&self) -> String {
        let events = self
            .events
            .lock_recovering("raft trace capture ring buffer");
        let mut out = String::new();
        let _ = writeln!(
            out,
            "=== EH-286 raft trace capture: {} event(s) (openraft + {RAFT_MODULE_TARGET}, DEBUG+) ===",
            events.len()
        );
        for event in events.iter() {
            let _ = writeln!(
                out,
                "[seq {:>6}] [t+{:>9.3}s] [{}] {:<5} {}: {}{}",
                event.seq,
                event.elapsed.as_secs_f64(),
                event.thread,
                event.level,
                event.target,
                event.message,
                event.fields,
            );
        }
        out
    }

    #[cfg(test)]
    fn clear(&self) {
        self.events
            .lock_recovering("raft trace capture ring buffer")
            .clear();
    }
}

/// A span's fields, formatted once at span creation and stashed as an extension so
/// every event inside it (including openraft's own leaf events, which often carry no
/// identity of their own) can inherit them. See [`append_ancestor_span_fields`].
struct SpanFields(String);

/// Collects a tracing event's (or span's) fields into a `message` field (the
/// conventional name `tracing`'s macros use for the format-args field) and a
/// `fields` string of every other field, each as ` name=value`.
#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: String,
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }
}

/// The `tracing_subscriber::Layer` that feeds a [`Ring`]. Composed with a
/// [`Targets`] filter in [`install_subscriber`] — this type itself captures
/// unconditionally whatever the filter lets through.
struct CaptureLayer {
    ring: &'static Ring,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let Some(span) = ctx.span(id) else {
            return;
        };
        span.extensions_mut().insert(SpanFields(visitor.fields));
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.fields;
        append_ancestor_span_fields(&ctx, event, &mut fields);
        self.ring.push(
            *event.metadata().level(),
            event.metadata().target(),
            thread_tag(),
            visitor.message,
            fields,
        );
    }
}

/// Fold every ancestor span's stashed [`SpanFields`] (root first) into `fields`, so
/// identity attached to a span (openraft's common pattern: `id`/`group` on the span
/// around a core loop, not repeated on each leaf event) survives on the leaf line.
fn append_ancestor_span_fields<S>(ctx: &Context<'_, S>, event: &Event<'_>, fields: &mut String)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let Some(scope) = ctx.event_scope(event) else {
        return;
    };
    for span in scope.from_root() {
        let extensions = span.extensions();
        let Some(SpanFields(span_fields)) = extensions.get::<SpanFields>() else {
            continue;
        };
        if !span_fields.is_empty() {
            fields.push(' ');
            fields.push_str(span_fields);
        }
    }
}

fn thread_tag() -> String {
    format!("{:?}", std::thread::current().id())
}

static RING: OnceLock<Ring> = OnceLock::new();
static SETUP: OnceLock<()> = OnceLock::new();

/// Install the capturing subscriber as the process's global default `tracing`
/// dispatcher, filtered to `openraft` + [`RAFT_MODULE_TARGET`] at `DEBUG`+.
/// `try_init` rather than `init`: a second, unrelated global-default install
/// elsewhere in the same test binary must not panic this one.
fn install_subscriber(ring: &'static Ring) {
    let targets = Targets::new()
        .with_target("openraft", Level::DEBUG)
        .with_target(RAFT_MODULE_TARGET, Level::DEBUG)
        .with_default(LevelFilter::OFF);
    let layer = CaptureLayer { ring }.with_filter(targets);
    let _ = tracing_subscriber::registry().with(layer).try_init();
}

/// Print the current dump to stderr — the whole action a cluster-test failure needs,
/// wired into the real panic hook below. Split out so its correctness ("does the
/// dump reach stderr") is legible by inspection, since a panic hook's output is not
/// convenient to assert on from a test.
fn dump_on_panic() {
    eprintln!("{}", render_dump());
}

/// Chain onto whatever panic hook is already installed (never replace it) and dump
/// the ring after it runs, so the normal panic message still prints first.
fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        previous(info);
        dump_on_panic();
    }));
}

/// Make trace capture the default for cluster tests: install the subscriber and
/// panic hook, exactly once per test binary. Cheap and safe to call from every
/// cluster test's node-startup path.
pub(crate) fn init() {
    SETUP.get_or_init(|| {
        let ring = RING.get_or_init(Ring::new);
        install_subscriber(ring);
        install_panic_hook();
    });
}

/// Render the current ring contents. Used by the panic hook, and directly by tests
/// that prove the mechanism works (see the `tests` module below).
pub(crate) fn render_dump() -> String {
    match RING.get() {
        Some(ring) => ring.render(),
        None => "=== EH-286 raft trace capture: not initialized (init() never ran) ===".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-bad input #1: an event on an unmatched target must NOT appear in the
    /// dump, proving the filter actually filters rather than capturing everything.
    /// Fully self-contained (its own `Ring` + subscriber via `with_default`, not the
    /// process global), so it cannot flake against another concurrently running
    /// cluster test.
    #[test]
    fn capture_layer_records_matched_targets_and_ignores_others() {
        let ring: &'static Ring = Box::leak(Box::new(Ring::new()));
        let targets = Targets::new()
            .with_target("openraft", Level::DEBUG)
            .with_target(RAFT_MODULE_TARGET, Level::DEBUG)
            .with_default(LevelFilter::OFF);
        let subscriber =
            tracing_subscriber::registry().with(CaptureLayer { ring }.with_filter(targets));

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug_span!(target: "openraft::replication", "group", group_id = 4, node_id = 9)
                .in_scope(|| {
                    tracing::debug!(target: "openraft::replication", "heartbeat send attempt");
                });
            tracing::warn!(
                target: "epistemic_graph::raft::multi",
                "placement group 4 has no current leader"
            );
            tracing::error!(target: "unrelated_module", "MARKER-SHOULD-NOT-APPEAR");
        });

        let dump = ring.render();
        assert!(dump.contains("heartbeat send attempt"));
        assert!(dump.contains("no current leader"));
        assert!(
            dump.contains("group_id=4"),
            "span identity must survive onto the leaf event: {dump}"
        );
        assert!(
            !dump.contains("MARKER-SHOULD-NOT-APPEAR"),
            "the target filter must reject non-raft events"
        );
    }

    /// Known-bad input #2: pushing past `CAPACITY` must evict the oldest entries,
    /// not grow without bound (EH-286's "bounded so a long test cannot exhaust
    /// memory" requirement).
    #[test]
    fn ring_buffer_evicts_oldest_when_over_capacity() {
        let ring = Ring::new();
        for i in 0..(CAPACITY + 3) {
            ring.push(
                Level::DEBUG,
                "openraft::test",
                "t".to_string(),
                format!("event-{i}"),
                String::new(),
            );
        }
        let dump = ring.render();
        assert!(
            !dump.contains("event-0"),
            "the oldest entries must have been evicted"
        );
        assert!(
            !dump.contains("event-2"),
            "exactly the first 3 (over-capacity) entries must be evicted"
        );
        assert!(
            dump.contains("event-3"),
            "the first entry still within capacity must survive"
        );
        assert!(
            dump.contains(&format!("event-{}", CAPACITY + 2)),
            "the newest entry must survive"
        );
    }

    /// Rendering preserves push order (oldest first) and stamps every line with a
    /// sequence number and elapsed time, so a dump reconstructs a causal chain
    /// rather than an unordered bag of lines.
    #[test]
    fn render_preserves_order_with_sequence_and_timestamp() {
        let ring = Ring::new();
        ring.push(
            Level::DEBUG,
            "openraft::replication",
            "t".to_string(),
            "first".to_string(),
            String::new(),
        );
        ring.push(
            Level::WARN,
            "epistemic_graph::raft::multi",
            "t".to_string(),
            "second".to_string(),
            String::new(),
        );
        let dump = ring.render();
        let first_at = dump.find("first").expect("first event present");
        let second_at = dump.find("second").expect("second event present");
        assert!(
            first_at < second_at,
            "events must render oldest-first: {dump}"
        );
        assert!(
            dump.contains("seq"),
            "each line must carry an ordering key: {dump}"
        );
        assert!(
            dump.contains("t+"),
            "each line must carry an elapsed timestamp: {dump}"
        );
    }

    /// End-to-end proof of the actual failure path: an event captured just before a
    /// deliberately failing assertion survives in the GLOBAL dump — the exact
    /// mechanism `cluster_cfg_with_groups` wires into every cluster test. The
    /// assertion is deliberately triggered and caught (never propagated), so this
    /// test proves the harness without itself failing the suite.
    #[test]
    fn a_deliberate_assertion_failure_leaves_its_causal_chain_in_the_global_dump() {
        init();
        let marker = format!(
            "EH286-PROOF-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        );
        tracing::debug!(target: "openraft::replication", "{marker} heartbeat reply: Err(Elapsed(()))");

        let panicked = std::panic::catch_unwind(|| {
            assert_eq!(
                1, 2,
                "deliberate known-bad input: simulated cluster assertion failure"
            );
        });
        assert!(
            panicked.is_err(),
            "the deliberate failure must actually panic"
        );

        let dump = render_dump();
        assert!(
            dump.contains(&marker),
            "the event emitted just before the failure must survive in the global dump"
        );
    }

    /// The ring itself, isolated from the global one, doesn't leak state across
    /// unrelated tests in this module.
    #[test]
    fn clear_empties_a_ring() {
        let ring = Ring::new();
        ring.push(
            Level::DEBUG,
            "openraft::test",
            "t".to_string(),
            "x".to_string(),
            String::new(),
        );
        ring.clear();
        let dump = ring.render();
        assert!(!dump.contains("x"));
        assert!(dump.contains("0 event(s)"));
    }
}
